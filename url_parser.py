"""
URL Parser and Database Population Script
This script crawls a university portal, extracts information about institutes, departments,
programs and subjects, and stores it in an SQLite database. It handles authentication,
request retries, and data normalization.
The script uses multiprocessing for parallel subject processing and includes data correction
capabilities for missing semester/evaluation information.
Configuration:
    Set in CONFIG dictionary including:
    - Portal URL and authentication credentials 
    - SSL certificate path
    - Database name
    - Operation flags for different data gathering steps
Database Structure:
    - institutes: Stores university institutes/faculties
    - departments: Stores departments under institutes
    - programs: Stores study programs under departments  
    - subjects: Stores subjects under programs with semester and evaluation info
Key Features:
    - NTLM authentication handling
    - Parallel processing with graceful interruption
    - Data normalization and cleaning
    - Smart semester/evaluation method inference
    - Progress resumption capability
    - Practice matching using fuzzy string matching
Dependencies:
    - requests, requests_ntlm: For HTTP requests and authentication
    - beautifulsoup4: For HTML/XML parsing
    - sqlite3: For database operations  
    - multiprocessing: For parallel processing
    - fuzzywuzzy: For fuzzy string matching
    - russian_names: For name generation
Author: RoCooEngi
"""
import sqlite3
import requests
from requests_ntlm import HttpNtlmAuth
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import re
import time
import random
from russian_names import RussianNames
import multiprocessing as mp
import signal
from fuzzywuzzy import process, fuzz
from pprint import pprint
from datetime import datetime

# Program configuration dictionary
CONFIG = {
    'MAIN_URL': 'https://portal3.sstu.ru/Pages/Default.aspx', # Main portal URL
    'USERNAME': 'SSTUEDUDOM\\220123',       # First username for authentication
    'PASSWORD': 'kamelot1',                 # First password
    'USERNAME1': 'SSTUEDUDOM\\220134',      # Second username for authentication
    'PASSWORD1': 'i2v0a0n4',                # Second password
    'SSL_CERTIFICATE': 'sstu_bundle.pem',   # SSL certificate file
    'DB_NAME': 'university.db',             # Database name
    'DB_OPERATIONS': {                      # Flags for database operations
        'all': False,
        'connection': False,
        'institutes': False,
        'departments': False,
        'programs': False,
        'subjects': False,
        'data correction': False,
        'students generator': True,
    },
    'SCHOLARSHIP_TOTAL': 60_000_000,            # Total scholarship fund
    'SCHOLARSHIP_SOCIAL': (2_000, 0.5),
    'SCHOLARSHIP_ACADEMIC': (11_500, 0.3),
    'EXAM_PROBABILITY': (0.25, 0.4, 0.25, 0.1), # Probabilities for grades 5,4,3,2
    'PASS_PROBABILITY': (0.75, 0.25),           # Probabilities for pass/fail
}

session_counter = 0 # Global session counter for retrying requests

stop_flag = mp.Value('b', False)  # Shared boolean flag for stopping multiprocessing

def init_worker(flag):
    '''Initialize each worker with access to shared stop flag.'''
    global stop_flag
    stop_flag = flag
    def handle_sigint(signum, frame):
        stop_flag.value = True
    signal.signal(signal.SIGINT, handle_sigint)
    # Note: this initializer is passed to multiprocessing.Pool so each
    # worker process receives a reference to the shared `stop_flag` value
    # and installs a SIGINT handler that sets the flag. This allows the
    # main process to request a graceful shutdown of workers.

def create_session(USERNAME=CONFIG['USERNAME'], PASSWORD=CONFIG['PASSWORD']):
    '''Creates and returns an NTLM-authenticated session.'''
    session = requests.Session()
    session.auth = HttpNtlmAuth(USERNAME, PASSWORD)
    return session

# `create_session` centralizes NTLM authentication creation so callers
# can get a ready-to-use `requests.Session` with credentials attached.

def url_parser(session: requests.Session, url: str, USERNAME=CONFIG['USERNAME'], PASSWORD=CONFIG['PASSWORD'], SSL_CERTIFICATE=CONFIG['SSL_CERTIFICATE']):
    '''Parses a URL and handles authentication errors and retries.'''
    global session_counter
    response = session.get(url, verify=SSL_CERTIFICATE)
    status = response.status_code
    if status != 200:
        print(f'Page {url} status: {status} - denied.')
        if status == 401:
            print('Trying to reconnect...')
            session_counter += 1
            if session_counter > 5:
                session_counter = 0
                return None
            session = create_session(USERNAME, PASSWORD)
            response = url_parser(session, url)
    print(f'Page {url} status: {status} - successful')
    return response

# `url_parser` is a small wrapper around `session.get` that attempts to
# recover from HTTP 401 (unauthorized) by recreating the session and
# retrying. It also uses `SSL_CERTIFICATE` when verifying TLS, which is
# required for the university portal's custom CA bundle.

def get_links(response: requests.Response):
    '''Extracts and returns all links from the HTML response as a dictionary.'''
    soup = BeautifulSoup(response.text, 'html.parser')
    links = {}
    for a in soup.find_all('a', href=True):
        link = a['href']
        if not link.startswith("http"):
            link = requests.compat.urljoin(CONFIG['MAIN_URL'], link)
        links[a.get_text(strip=True)] = link
    return links

# `get_links` returns a mapping of link text -> absolute URL. It uses
# `MAIN_URL` as the base for resolving relative links. Link text is used
# as the dictionary key because the portal's navigation relies on
# descriptive anchor text.

def xml_extractor(response: requests.Response):
    '''Finds and returns the XML file URL from the HTML response.'''
    soup = BeautifulSoup(response.text, 'html.parser')
    for a in soup.find_all(attrs={'o:webquerysourcehref': True}):
        data = a['o:webquerysourcehref']
        if 'XMLDATA' in data:
            return data

# The portal exposes list data via an attribute named
# `o:webquerysourcehref` that points to an XML export. `xml_extractor`
# searches for elements with that attribute and returns the first link
# containing 'XMLDATA'.

def xml_parser(response: requests.Response, key: str):
    '''Parses XML response and extracts values by key.'''
    soup = BeautifulSoup(response.text, 'lxml-xml')
    return [row.get(key) for row in soup.find_all('z:row') if row.get(key)]

# `xml_parser` expects SharePoint-like XML where each record is a
# `z:row` element and values are stored as attributes. Caller provides
# the attribute `key` to extract (e.g. subject name attribute).
        
def pattern_links(links: dict, pattern: re.Pattern):
    '''Filters links dictionary by regex pattern.'''
    return {name: link for name, link in links.items() if re.search(pattern, link)}

# Utility to filter the `get_links` result using a compiled regex. This
# keeps higher-level code concise when selecting institute/department
# and program links.

def clean_text(t):
    '''Cleans and normalizes text by removing extra spaces.'''
    return re.sub(r"\s+", " ", t.strip()) if t else ""

# Normalizes whitespace and guards against `None` values. Used widely
# to sanitize scraped table cell contents.

def parse_subject(session: requests.Session, url: str):
    '''Parses subject page and extracts semester information.'''
    response = session.get(url, verify=CONFIG['SSL_CERTIFICATE'])
    soup = BeautifulSoup(response.text, "html.parser")
    result = {}

    # Find table with subject parameters
    table = soup.find("table", class_="ms-listviewtable")
    if table:
        headers = [clean_text(th.get_text()) for th in table.find_all("th")]
        data_row = table.find("tr", class_="ms-itmhover") or table.find("tr", class_="")
        if data_row:
            values = [clean_text(td.get_text()) for td in data_row.find_all("td")]
            if len(headers) == len(values):
                row_map = dict(zip(headers, values))
                result.update(row_map)
    return result['Семестр']

# `parse_subject` looks for a `ms-listviewtable` table and maps headers
# to values for the highlighted row. It returns the value under the
# 'Семестр' header. Note: this will raise a KeyError if 'Семестр' is
# missing — callers should be prepared to handle exceptions.

def pause():
    '''Pauses execution for a random short interval to avoid detection.'''
    time.sleep(random.uniform(0.5, 2))

# Randomized short sleep to reduce load and avoid triggering rate
# limiting or detection on the portal.

def log_request_error(table: str):
    '''Logs error when unable to request a table due to missing permission.'''
    print(f'Unable to request "{table}": permission is missing')

# Small helper to keep messaging consistent when DB_OPERATIONS flags
# are turned off.

def is_icon_td(td):
    '''Checks if a table cell contains only an icon (image or link without text).'''
    txt = clean_text(td.get_text(" ", strip=True))
    if txt:
        return False
    imgs = td.find_all("img")
    links = td.find_all("a")
    if imgs and not any(clean_text(a.get_text()) for a in links):
        return True
    return False

# The program's HTML tables sometimes include leading icon cells that do
# not correspond to data columns. `is_icon_td` detects such cells so the
# parser can skip them and align values with headers.

def parse_program_page(session: requests.Session, url: str):
    '''Parses a program page and returns normalized data table.'''
    response = url_parser(session, url)
    if not response:
        return None
    soup = BeautifulSoup(response.text, "html.parser")
    tables = soup.find_all("table", class_="ms-listviewtable")
    all_parsed = []

    for table in tables:
        # find headers
        header_tr = table.find("tr", class_=re.compile(r"ms-viewheadertr|ms-headerrow|ms-viewheader"))
        headers = []
        for h in table.find_all(class_=re.compile(r"ms-vh")):
            t = clean_text(h.get_text(" ", strip=True))
            if t and t not in headers:
                headers.append(t)
        if not headers and header_tr:
            headers = [clean_text(x.get_text(" ", strip=True)) for x in header_tr.find_all(['th', 'td']) if clean_text(x.get_text(" ", strip=True))]

        # Collect rows
        rows = []
        if header_tr:
            r = header_tr.find_next_sibling("tr")
            while r:
                tds = r.find_all("td")
                if tds:
                    vals = [clean_text(td.get_text(" ", strip=True)) for td in tds]
                    tds_copy = list(tds)
                    while headers and len(vals) > len(headers) and tds_copy and is_icon_td(tds_copy[0]):
                        tds_copy.pop(0)
                        vals.pop(0)
                    if headers:
                        if len(vals) < len(headers):
                            vals += [""] * (len(headers) - len(vals))
                        if len(vals) > len(headers):
                            vals = vals[:len(headers)]
                        rows.append(dict(zip(headers, vals)))
                r = r.find_next_sibling("tr")
        else:
            for tr in table.find_all("tr"):
                tds = tr.find_all("td")
                if not tds:
                    continue
                vals = [clean_text(td.get_text(" ", strip=True)) for td in tds]
                gen = [f"col_{i+1}" for i in range(len(vals))]
                rows.append(dict(zip(gen, vals)))

        if rows:
            all_parsed.append({"headers": headers, "rows": rows, "summary": table.get("summary", "")})

    # Choose the table with 'семестр' or 'курс'
    selected = None
    for t in all_parsed:
        hdrs_text = " ".join(t["headers"]).lower()
        if "семестр" in hdrs_text or "курс" in hdrs_text:
            selected = t
            break
    if selected is None and all_parsed:
        selected = all_parsed[0]

    # Row normalization
    norm_rows = []
    if selected:
        for r in selected["rows"]:
            nr = dict(r)
            def find_key(sub):
                for k in r.keys():
                    if sub in k.lower():
                        return k
                return None
            k = find_key("семестр")
            if k:
                nr["Семестр"] = r[k]
            k = find_key("количество лек")
            if k:
                nr["Количество лекций"] = r[k]
            k = find_key("лаборат") or find_key("практическ")
            if k:
                nr["Кол-во лаб/практ"] = r[k]
            k = find_key("отчетност") or find_key("форма")
            if k:
                nr["Отчетность"] = r[k]
            k = find_key("лектор")
            if k:
                nr["Преподаватель-лектор"] = r[k]
            k = find_key("ассистент")
            if k:
                nr["Преподаватели-ассистенты"] = r[k]
            v = nr.get("Кол-во лаб/практ", "")
            m = re.match(r'^\s*(\d+)\s*/\s*(\d+)\s*$', v)
            if m:
                nr["Лаб"] = m.group(1)
                nr["Практ"] = m.group(2)
            norm_rows.append(nr)
    return norm_rows

# `parse_program_page` is the most complex HTML parser here. It tries
# to robustly extract tabular data from SharePoint-like tables by:
# - locating header rows (several class patterns are supported)
# - collecting each data row and aligning values with headers
# - skipping icon-only cells that would otherwise misalign columns
# - creating normalized rows with predictable keys like "Семестр",
#   "Отчетность", and split lab/practice counts in "Лаб" and "Практ".
#
# The function returns a list of normalized row dicts or None if the
# page couldn't be retrieved.

def subject_multi_process(programs: list, USERNAME:str, PASSWORD: str):
    '''Multiprocess function for parsing subjects for a list of programs.'''
    session = create_session(USERNAME, PASSWORD)
    connection = sqlite3.connect(CONFIG['DB_NAME'])
    cursor = connection.cursor()
    
    progs_subjects = []
    
    for prog_id, prog_url in programs:
        if stop_flag.value:  # Check stop flag at the start of each iteration
            print(f"Process {mp.current_process().name} stopping due to stop_flag")
            break
        
        response_prog = url_parser(session, prog_url)
        if not response_prog:
            continue
        xml_link = xml_extractor(response_prog)
        response_xml = url_parser(session, xml_link)
        if not response_xml:
            continue
        subjects = xml_parser(response_xml, 'ows__x041d__x0430__x0438__x043c__x04')
        subjects = {sub.strip(' /'): link.strip() for raw_value in subjects for link, sub in [raw_value.split(',', 1)]}
        
        # Check existing subjects for this program
        cursor.execute('SELECT name, semester FROM subjects WHERE program_id = ?', (prog_id,))
        existing_subjects = {(name, semester) for name, semester in cursor.fetchall()}
        
        new_subjects = []
        for sub_name, sub_url in subjects.items():
            semester = 0
            eval_method = ''
            try:
                if stop_flag.value:  # Check stop flag in inner loop
                    print(f"Process {mp.current_process().name} stopping due to stop_flag")
                    break
                # Parse subject page
                norm_rows = parse_program_page(session, sub_url)
                for row in norm_rows:
                    if row.get("Семестр"):
                        try:
                            semester = int(row["Семестр"])
                        except (ValueError, TypeError):
                            print(f"Invalid semester value for {sub_name}: {row['Семестр']}")
                            semester = 0
                        break
                for row in norm_rows:
                    if row.get("Отчетность"):
                        try:
                            eval_method = row["Отчетность"]
                        except (ValueError, TypeError):
                            print(f"Invalid evaluation method value for {sub_name}: {row['Отчетность']}")
                            eval_method = ''
                        break
            except Exception as e:
                print(f"Error parsing subject {sub_name} at {sub_url}: {str(e)}")
                semester = 0
                eval_method = ''
            # Check if subject already exists with the same semester
            if (sub_name, semester) not in existing_subjects:
                new_subjects.append((sub_name, semester, eval_method, sub_url, prog_id))
                print(f'Subject: {sub_name}, Semester: {semester}, Eval method: {eval_method}, Program id: {prog_id}')
        
        if new_subjects:
            progs_subjects.append(new_subjects)
        else:
            print(f'No new subjects for program {prog_id}')
        pause()
    connection.close()
    return progs_subjects

# `subject_multi_process` is designed to run inside a worker process.
# It creates its own authenticated session and database connection (DB
# connections cannot be shared safely across processes). For each program
# it fetches associated subjects via XML, parses each subject page to
# extract semester and evaluation method, skips already-saved subjects,
# and returns the list of new subjects to be inserted by the parent.

def extract_semester_from_name(sub_name):
    '''Extracts semester number from subject name using regex.'''
    # pattern like "(Nth semester)", "(N semester)", "N semester"
    pattern = r'\b(\d{1,2})(?:-й|-ой|-го|-му|-м|-й\s+|-го\s+)?\s*семестр'
    match = re.search(pattern, sub_name, re.IGNORECASE)
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            return None
    return None

# This helper tries to infer the semester directly from a subject's
# name (e.g. "Практика 3 семестр"). It's used during data correction
# when the semester value is missing.

def determine_program_type(prog_name):
    '''Determines the program type and max semesters based on keywords or subject count.'''
    prog_name = prog_name.lower()
    if any(keyword in prog_name for keyword in MASTER_KEYWORDS):
        return 'магистратура', 4
    elif any(keyword in prog_name for keyword in BACHELOR_KEYWORDS):
        return 'бакалавриат', 8
    elif any(keyword in prog_name for keyword in SPECIALIST_KEYWORDS):
        return 'специалитет', 11
    else:
        cursor.execute('SELECT COUNT(*) FROM subjects WHERE program_id = ?', (prog_id,))
        subject_count = cursor.fetchone()[0]
        if subject_count > 80:
            return 'специалитет', 11
        elif subject_count > 40:
            return 'бакалавриат', 8
        else:
            return 'магистратура', 4

# Determines likely program type by checking known keywords first.
# If keywords are absent, it falls back to the number of subjects in
# the DB to heuristically decide. Note: this function references
# `cursor` and `prog_id` from the calling scope in the original code;
# therefore it expects to be called where those variables are defined.

def match_practice(sub_name, practice_dict):
    '''Matches practice name with template using fuzzy matching.'''
    sub_name = sub_name.lower().strip()
    best_match = process.extractOne(sub_name, practice_dict.keys(), scorer=fuzz.token_sort_ratio)
    if best_match and best_match[1] > 80:
        return practice_dict[best_match[0]], best_match[0]
    return None, None

# Uses `fuzzywuzzy` to match a subject name against a dictionary of
# known practice templates (e.g. "преддипломная практика") and returns
# the mapped semester if the match is confident.

def determine_eval_method(sub_name, semester, prog_type, is_practice_or_attestation):
    '''Defines the type of reporting (exam, credit, or evaluation) for a subject.'''
    sub_name = sub_name.lower().strip()
    if is_practice_or_attestation:
        return 'Оценка'
    else:
        max_semesters = {'бакалавриат': 8, 'магистратура': 4, 'специалитет': 11}
        if semester >= max_semesters[prog_type] - 1:
            return 'Экзамен'
        return 'Зачет'

# Simple rule-based decision: practices/attestations are graded
# ('Оценка'), final semester subjects are exams ('Экзамен'), others
# default to pass/fail ('Зачет').

def make_abbr(text: str) -> str:
    '''Generate an abbreviation for a program name.'''
    if not text:
        return ''

    # try parentheses first (take inner quoted / parenthesized abbreviation/title)
    match = re.search(r"\(([^\)]+)\)", text)
    if match:
        raw = match.group(1).strip()
        # remove trailing year/mode tokens like 2018, очная, заочная, з/о etc.
        raw = re.sub(r"\b(очная|заочная|з/о|о/о|201\d|20\d{2})\b", "", raw, flags=re.IGNORECASE).strip()
        # if parentheses content looks like an abbreviation or short code, use it
        if 1 <= len(raw) <= 12 and re.search(r'[A-Za-zА-Яа-я0-9]', raw):
            # extract letters and digits, keep uppercase form
            ab = ''.join(re.findall(r'[A-Za-zА-Я0-9]', raw))
            if ab:
                return ab.upper()

    # handle quoted main titles ("..." or «...» or '...') preferring their initials
    quote_match = re.search(r'["\'\u00AB\u00BB](.+?)["\'\u00AB\u00BB]', text)
    if quote_match:
        quoted = quote_match.group(1)
        # build initials from quoted title first
        parts_q = re.split(r'[\s,;/]+', quoted)
        initials_q = [re.sub(r"[^A-Za-zА-Яа-я0-9]", '', w)[:1] for w in parts_q if w and len(re.sub(r"[^A-Za-zА-Яа-я0-9]", '', w))>0]
        if initials_q:
            return ''.join(initials_q)[:6].upper()

    # words to ignore when building initials
    stopwords = {
        'и','в','на','по','с','к','из','для','под','о','об','при',
        'бакалавр','бакалавриат','магистр','магистратура','специалитет',
        'программа','образования','по','направление','направления',
        'учебная','производственная','практика','практическая'
    }

    # normalize repeated dashes and remove stray punctuation
    text_clean = re.sub(r'-{2,}', '-', text)
    # remove trailing year/mode tokens (common noise)
    text_clean = re.sub(r"\b(очная|заочная|з/о|о/о|201\d|20\d{2})\b", "", text_clean, flags=re.IGNORECASE)
    # split on whitespace and separators and keep hyphenated parts
    parts = re.split(r'[\s,;/]+', text_clean)
    initials = []
    for p in parts:
        if not p:
            continue
        # split hyphenated components
        comps = re.split(r'[-–—]', p)
        for c in comps:
            # strip punctuation
            cstr = re.sub(r"[^A-Za-zА-Яа-я0-9]", '', c)
            if not cstr:
                continue
            low = cstr.lower()
            # skip single-letter segments that look like course codes (like 'б1', 'б8') unless meaningful
            if low in stopwords or re.fullmatch(r'[бвмс]\d+', low):
                continue
            # take first letter (prefer uppercase if present later we upper())
            initials.append(cstr[0])

    if not initials:
        # fallback: take all uppercase letters from the text
        letters = re.findall(r'[A-ZА-Я0-9]', text)
        return ''.join(letters).upper()

    # form abbreviation from initials (letters only), limit length to 6
    initials_letters = [ch for ch in initials if re.match(r'[A-Za-zА-Яа-я]', ch)]
    abbr = ''.join(initials_letters)[:6].upper()

    # If abbreviation has fewer than 2 letters, try other fallbacks
    if len(abbr) < 2:
        # 1) Try to collect initial letters from all words in the original text
        words = re.findall(r'[A-Za-zА-Яа-я]+', text)
        more = ''.join(w[0] for w in words if w)
        if more:
            candidate = (abbr + more).upper()
            if len(candidate) >= 2:
                return candidate[:6]

        # 2) Fallback to extracting uppercase letters from the text (letters only)
        up_letters = ''.join(re.findall(r'[A-ZА-Я]', text))
        if len(up_letters) >= 2:
            return up_letters[:6]

        # 3) If we have a single letter, duplicate it to make two letters
        if len(abbr) == 1:
            return (abbr * 2)[:6]

        # 4) As a last resort, return first two alphabetic characters found anywhere
        all_letters = ''.join(re.findall(r'[A-Za-zА-Яа-я]', text))
        if len(all_letters) >= 2:
            return all_letters[:6].upper()

        # Nothing usable found — return empty string
        return ''

    return abbr


if __name__ == '__main__':
    # Create database and tables if not exist
    connection_db = sqlite3.connect('university.db')
    cursor = connection_db.cursor()
    with open('tables_init.sql', 'r', encoding='utf-8') as file:
        cursor.executescript(file.read())
    connection_db.commit()

    # Connect to the main portal if enabled
    if CONFIG['DB_OPERATIONS']['connection'] or CONFIG['DB_OPERATIONS']['all']:
        session = create_session()

    # Load main page and get all institutes
    if not (CONFIG['DB_OPERATIONS']['institutes'] or CONFIG['DB_OPERATIONS']['all']):
        log_request_error('institutes')
    else:
        response_main = url_parser(session, CONFIG['MAIN_URL'])
        first_level_links = get_links(response_main)
        first_level_pattern = re.compile(r"Facult/[A-Z]+(?=/|$)")
        links_inst = pattern_links(first_level_links, first_level_pattern)
        
        if not links_inst:
            print('Dict of institutes is empty! Check the url parser')
            exit(1)
        
        # Get existing institutes from DB
        cursor.execute('SELECT name, url FROM institutes')
        existing_institutes = dict(cursor.fetchall())
        
        # Filter new or updated institutes
        new_institutes = [(name, url) for name, url in links_inst.items() if name not in existing_institutes or existing_institutes[name] != url]
        if new_institutes:
            cursor.executemany('INSERT OR REPLACE INTO institutes (name, url) VALUES (?, ?)', new_institutes)
            connection_db.commit()
            print(f'{len(new_institutes)} new or updated institutes have been saved')
        else:
            print('No new or updated institutes to save')
        pause()

# The block above scrapes top-level institute links and updates the
# `institutes` table. It uses `INSERT OR REPLACE` to update existing
# records with changed URLs while preserving IDs for resumption.

    # Load institute page and get departments
    if not (CONFIG['DB_OPERATIONS']['departments'] or CONFIG['DB_OPERATIONS']['all']):
        log_request_error('departments')
    else:
        # Get the last processed institute to resume
        cursor.execute('SELECT institute_id FROM departments ORDER BY id DESC LIMIT 1')
        last_institute = cursor.fetchone()
        last_institute_id = last_institute[0] if last_institute else 0
        
        # Select institutes starting from the last processed one
        cursor.execute('SELECT id, url FROM institutes WHERE id >= ? ORDER BY id', (last_institute_id,))
        institutes = cursor.fetchall()
        
        for inst_id, inst_url in institutes:
            response_inst = url_parser(session, inst_url)
            if not response_inst:
                continue
            second_level_links = get_links(response_inst)
            second_level_pattern = re.compile(r"/Facult/[A-Z]+/[A-Z]+(?:/default\.aspx)?$")
            links_dep = pattern_links(second_level_links, second_level_pattern)
            if not links_dep:
                print(f'No departments found for institute {inst_id}')
                continue
            
            # Check existing departments for this institute
            cursor.execute('SELECT name, url FROM departments WHERE institute_id = ?', (inst_id,))
            existing_departments = dict(cursor.fetchall())
            
            # Filter new or updated departments
            new_departments = [(name, url, inst_id) for name, url in links_dep.items() 
                            if name not in existing_departments or existing_departments[name] != url]
            
            if new_departments:
                cursor.executemany('INSERT OR REPLACE INTO departments (name, url, institute_id) VALUES (?, ?, ?)', 
                                new_departments)
                connection_db.commit()
                print(f'{len(new_departments)} new or updated departments saved for institute {inst_id}')
            else:
                print(f'No new or updated departments for institute {inst_id}')
            pause()
        print('Departments data has been saved')

# This section iterates institutes and collects department links,
# inserting them into `departments`. The code queries the last
# processed `institute_id` to support resuming from where a previous
# run left off.

    # Load department page and get programs
    if not (CONFIG['DB_OPERATIONS']['programs'] or CONFIG['DB_OPERATIONS']['all']):
        log_request_error('programs')
    else:
        # Get the last processed department to resume
        cursor.execute('SELECT department_id FROM programs ORDER BY id DESC LIMIT 1')
        last_department = cursor.fetchone()
        last_department_id = last_department[0] if last_department else 0
        
        # Select departments starting from the last processed one
        cursor.execute('SELECT id, url FROM departments WHERE id >= ? ORDER BY id', (last_department_id,))
        departments = cursor.fetchall()
        
        for dep_id, dep_url in departments:
            response_dep = url_parser(session, dep_url)
            if not response_dep:
                continue
            third_level_links = get_links(response_dep)
            third_level_pattern = re.compile(r"/\d{2}\.\d{2}\.\d{2}[^/]*(?:/default\.aspx)?$")
            links_prog = pattern_links(third_level_links, third_level_pattern)
            if not links_prog:
                print(f'No programs found for department {dep_id}')
                continue
            
            # Check existing programs for this department
            cursor.execute('SELECT name, url FROM programs WHERE department_id = ?', (dep_id,))
            existing_programs = dict(cursor.fetchall())
            
            # Filter new or updated programs
            new_programs = [(name, url, dep_id) for name, url in links_prog.items() 
                            if name not in existing_programs or existing_programs[name] != url]
            
            if new_programs:
                cursor.executemany('INSERT OR REPLACE INTO programs (name, url, department_id) VALUES (?, ?, ?)', 
                                new_programs)
                connection_db.commit()
                print(f'{len(new_programs)} new or updated programs saved for department {dep_id}')
            else:
                print(f'No new or updated programs for department {dep_id}')
            pause()
        print('Programs data has been saved')

# This block finds program pages under each department and stores them
# in the `programs` table. Pattern matching focuses on program codes
# that follow the 'NN.NN.NN' structure used by the portal.

    # Load program page and get all subjects
    if not (CONFIG['DB_OPERATIONS']['subjects'] or CONFIG['DB_OPERATIONS']['all']):
        log_request_error('subjects')
    else:        
        # Get the last processed program to resume
        cursor.execute('SELECT program_id FROM subjects ORDER BY id DESC LIMIT 1')
        last_program = cursor.fetchone()
        last_program_id = last_program[0] if last_program else 0
        
        # Select programs starting from the last processed one
        cursor.execute('SELECT id, url FROM programs WHERE id >= ? ORDER BY id', (last_program_id,))
        programs = cursor.fetchall()
        programs_half = [(programs[:len(programs)//2], CONFIG['USERNAME1'], CONFIG['PASSWORD1']),
                         (programs[len(programs)//2:], CONFIG['USERNAME'], CONFIG['PASSWORD'])]
        
        stop_flag = mp.Value('b', False)
        
        # Signal handler for the main process
        def handler(sig, frame):
            print("\n[!] Interrupt received, signaling processes to stop...")
            stop_flag.value = True
        
        signal.signal(signal.SIGINT, handler)
        
        with mp.Pool(processes=len(programs_half), initializer=init_worker, initargs=(stop_flag,)) as pool:
            try:
                results = pool.starmap(subject_multi_process, programs_half)
            except KeyboardInterrupt:
                print("[!] Waiting for processes to stop gracefully...")
                pool.close()  # Prevent new tasks from starting
                pool.join()   # Wait for all processes to complete
                results = []
            finally:
                pool.close()
                pool.join()
                
        all_results = [item for result in results for item in result]
        
        # Save the results to the database
        for new_subjects in all_results:
            if new_subjects:
                cursor.executemany('INSERT INTO subjects (name, semester, eval_method, url, program_id) VALUES (?, ?, ?, ?, ?)', new_subjects)
                connection_db.commit()
                print(f'{len(new_subjects)} new subjects saved')

# Subject fetching is parallelized using a Pool of workers, each with
# its own credentials and DB connection. Results are collected and
# inserted by the main process to avoid DB concurrency issues.

    # Update zero data subjects' semesters and evaluation methods
    if not (CONFIG['DB_OPERATIONS']['data correction'] or CONFIG['DB_OPERATIONS']['all']):
        print('Unable to update semesters and evaluation methods: permission is missing')
    else:
        # practice templates for different types of programs
        BACHELOR_PRACTICES = {
            '1 учебная практика': 2,
            '2 учебная практика': 4,
            'производственная (технологическая) практика': 6,
            'производственная практика (нир)': 7,
            'преддипломная практика': 8,
            'государственная итоговая аттестация': 8
        }
        MASTER_PRACTICES = {
            'учебная практика': 1,
            'производственная практика (технологическая)': 2,
            'производственная практика (педагогическая)': 3,
            'научно-исследовательская работа': 3,
            'преддипломная практика': 4,
            'государственная итоговая аттестация': 4
        }
        SPECIALIST_PRACTICES = {
            '1-ая учебная практика (ознакомительная)': 2,
            '2-ая учебная практика (обмерная)': 4,
            '3-ая учебная практика (геодезическая)': 6,
            '1-ая производственная практика (технологическая)': 6,
            '2-ая производственная практика (исследовательская)': 7,
            '3-я производственная практика (проектно-исследовательская)': 10,
            'преддипломная практика': 11,
            'государственная итоговая аттестация': 11
        }

        # Keyword lists for determining program type
        BACHELOR_KEYWORDS = ['бакалавр', 'бакалавриат']
        MASTER_KEYWORDS = ['магистр', 'магистратура']
        SPECIALIST_KEYWORDS = ['специалитет', 'специалист']

        cursor.execute('SELECT id, name, semester, eval_method, program_id FROM subjects WHERE semester = 0 OR eval_method = ""')
        rows_to_update = cursor.fetchall()

        if rows_to_update:
            for sub_id, sub_name, sub_sem, sub_eval, prog_id in rows_to_update:
                cursor.execute('SELECT name FROM programs WHERE id = ?', (prog_id,))
                prog_name = cursor.fetchone()[0]

                prog_type, max_semesters = determine_program_type(prog_name)

                new_sem = sub_sem
                is_practice_or_attestation = False
                if sub_sem == 0:
                    extracted_sem = extract_semester_from_name(sub_name)
                    if extracted_sem is not None and extracted_sem <= max_semesters:
                        new_sem = extracted_sem
                    else:
                        if prog_type == 'бакалавриат':
                            new_sem, matched_name = match_practice(sub_name, BACHELOR_PRACTICES)
                        elif prog_type == 'магистратура':
                            new_sem, matched_name = match_practice(sub_name, MASTER_PRACTICES)
                        else:
                            new_sem, matched_name = match_practice(sub_name, SPECIALIST_PRACTICES)
                        
                        if new_sem is not None:
                            is_practice_or_attestation = True
                        else:
                            new_sem = random.randint(1, max_semesters)

                new_eval = sub_eval
                if not sub_eval:
                    new_eval = determine_eval_method(sub_name, new_sem, prog_type, is_practice_or_attestation)

                cursor.execute('UPDATE subjects SET semester = ?, eval_method = ? WHERE id = ?', 
                            (new_sem, new_eval, sub_id))
                print(f'Subject: {sub_name}, Semester: {new_sem}, Eval method: {new_eval}, Program: {prog_name}')

            connection_db.commit()
            print('Semesters and evaluation methods have been updated!')
        else:
            print('No semester or evaluation method data to update')
    
    # connection_db.close()
    
    # Update zero data subjects' semesters and evaluation methods
    if not (CONFIG['DB_OPERATIONS']['students generator'] or CONFIG['DB_OPERATIONS']['all']):
        print('Unable to fill students, grades and groups table: permission is missing')
    else:
        cursor.execute('SELECT id, name FROM programs ORDER BY id')
        programs = cursor.fetchall()
        for prog_id, prog_name in programs:
            cursor.execute('SELECT COUNT(*) FROM groups WHERE program_id = ?', (prog_id,))
            if cursor.fetchone()[0] == 0:
                cursor.execute('SELECT semester FROM subjects WHERE program_id = ?', (prog_id,))
                semesters_data = cursor.fetchall()
                semesters = max({row[0] for row in semesters_data if row[0]}) if semesters_data else 0
                if not semesters:
                    print(f'Program "{prog_name}" has no subjects with valid semesters, skipping group generation')
                    continue
                elif semesters <= 4:
                    education_type = 'м'
                    group_count = 2
                elif semesters <= 8:
                    education_type = 'б'
                    group_count = 4
                else:
                    education_type = 'с'
                    group_count = 6
                group_name = f"{education_type}-{make_abbr(prog_name)}"
                for group in range(1, group_count + 1):
                    cursor.execute('INSERT INTO groups (name, course_year, program_id) VALUES (?, ?, ?)', 
                                (f"{group_name}-{group}", group, prog_id))
                print(f'Program "{prog_name}" with {semesters} semesters: created {group_count} groups of type "{group_name}"')
                connection_db.commit()
            else:
                print(f'Program "{prog_name}" already has groups, skipping')
        
        cursor.execute('SELECT id FROM groups ORDER BY id')
        groups = [i[0] for i in cursor.fetchall()]
        if not groups:
            print('No groups available, cannot generate students')
        else:
            cursor.execute('SELECT COUNT(*) FROM students')
            if cursor.fetchone()[0] != 0:
                print('Students table is not empty, skipping student generation')
            else:
                student_id = 200000
                for group_id in groups:
                    student_count = random.randint(15, 25)
                    for student in range(1, student_count + 1):
                        student_name = RussianNames().get_person()
                        cursor.execute('INSERT INTO students (id, name, group_id) VALUES (?, ?, ?)', (student_id, student_name, group_id))
                        print(f'Created student {student_name} with ID {student_id} in group {group_id}')
                        student_id += 1
                    connection_db.commit()
                print('Students have been generated and saved')
            
            cursor.execute('SELECT id, group_id FROM students ORDER BY id')
            students = [i for i in cursor.fetchall()]
            if not students:
                print('Students table is empty, skipping grades generation')
            else:
                cursor.execute('SELECT COUNT(*) FROM grades')
                if cursor.fetchone()[0] != 0:
                    print('Grades table is not empty, skipping grades generation')
                else:
                    for student_id, group_id in students:
                        # get program and course_year for the group
                        cursor.execute('SELECT program_id, course_year FROM groups WHERE id = ?', (group_id,))
                        res = cursor.fetchone()
                        if not res:
                            print(f'Group {group_id} not found for student {student_id}, skipping')
                            continue
                        prog_id, course_year = res

                        # determine student's current semester based on PC time
                        # Russian academic year: 1st sem = Sep..Jan, 2nd sem = Feb..Jul, Aug treated as summer (use 2nd)
                        now = datetime.now()
                        month = now.month
                        if month in (9, 10, 11, 12, 1):
                            sem_in_course = 1
                        else:
                            # months 2..7 => second semester, 8 (Aug) => summer -> use second semester
                            sem_in_course = 2

                        if not course_year or course_year < 1:
                            print(f'Invalid course_year {course_year} for group {group_id}, skipping student {student_id}')
                            continue
                        student_semester = 2 * course_year - 1 if sem_in_course == 1 else 2 * course_year

                        # get program subjects
                        cursor.execute('SELECT id, semester, eval_method FROM subjects WHERE program_id = ?', (prog_id,))
                        subjects = cursor.fetchall()
                        if not subjects:
                            print(f'Program {prog_id} has no subjects, skipping student {student_id}')
                            continue

                        for subj_id, semester, eval_method in subjects:
                            # treat missing/zero semester as future (insert NULL)
                            if not semester or semester > student_semester:
                                grade = None
                            else:
                                if eval_method in ('Экзамен', 'Оценка'):
                                    grade = random.choices((5, 4, 3, 2), weights=CONFIG['EXAM_PROBABILITY'])[0]
                                else:
                                    # for pass/fail store 1 for pass, 0 for fail
                                    grade = random.choices((1, 0), weights=CONFIG['PASS_PROBABILITY'])[0]

                            cursor.execute('INSERT INTO grades (student_id, subject_id, grade) VALUES (?, ?, ?)',
                                           (student_id, subj_id, grade))
                        connection_db.commit()
                        print(f'Generated grades for student ID {student_id} (current semester: {student_semester})')
                    print('Grades have been generated and saved')
            
            cursor.execute('SELECT id, name, group_id FROM students ORDER BY id')
            students = cursor.fetchall()
            if not students:
                print('No students available, cannot generate scholarships')
            else:
                remaining = CONFIG['SCHOLARSHIP_TOTAL']
                social_amt, social_prob = CONFIG['SCHOLARSHIP_SOCIAL']
                academic_amt, academic_prob = CONFIG['SCHOLARSHIP_ACADEMIC']
                awarded_social = 0
                awarded_academic = 0

                now = datetime.now()
                month = now.month
                sem_in_course = 1 if month in (9, 10, 11, 12, 1) else 2

                for student_id, student_name, group_id in students:
                    # get program and course_year for group
                    cursor.execute('SELECT program_id, course_year FROM groups WHERE id = ?', (group_id,))
                    res = cursor.fetchone()
                    if not res:
                        print(f'Group {group_id} not found for student {student_id}, skipping scholarship check')
                        continue
                    prog_id, course_year = res
                    if not course_year or course_year < 1:
                        print(f'Invalid course_year {course_year} for group {group_id}, skipping student {student_id}')
                        continue

                    student_semester = 2 * course_year - 1 if sem_in_course == 1 else 2 * course_year

                    # fetch grades for subjects in the current semester for this student
                    cursor.execute(
                        '''SELECT g.grade
                           FROM grades g
                           JOIN subjects s ON g.subject_id = s.id
                           WHERE g.student_id = ? AND s.semester = ?''',
                        (student_id, student_semester)
                    )
                    grade_rows = [r[0] for r in cursor.fetchall()]

                    if not grade_rows:
                        # no grades -> scholarship = 0
                        cursor.execute('UPDATE students SET scholarship = 0 WHERE id = ?', (student_id,))
                        connection_db.commit()
                        print(f'Student {student_name} (ID {student_id}) has no grades for semester {student_semester}, scholarship set to 0')
                        continue

                    # if any grade is NULL (None) treat as not eligible -> scholarship = 0
                    if any(g is None for g in grade_rows):
                        cursor.execute('UPDATE students SET scholarship = 0 WHERE id = ?', (student_id,))
                        connection_db.commit()
                        print(f'Student {student_name} (ID {student_id}) has missing grades for semester {student_semester}, scholarship set to 0')
                        continue

                    # check for fails (0), twos (2) and threes (3) -> scholarship = 0
                    if any(g in (0, 2, 3) for g in grade_rows):
                        cursor.execute('UPDATE students SET scholarship = 0 WHERE id = ?', (student_id,))
                        connection_db.commit()
                        print(f'Student {student_name} (ID {student_id}) is NOT eligible (has 0,2 or 3) for semester {student_semester}, scholarship set to 0')
                        continue

                    # eligible for social scholarship (amount + chance)
                    student_awarded_total = 0
                    # check funds and random chance
                    if remaining >= social_amt and random.random() < social_prob:
                        remaining -= social_amt
                        awarded_social += social_amt
                        student_awarded_total += social_amt
                        cursor.execute('UPDATE students SET scholarship = ? WHERE id = ?', (student_awarded_total, student_id))
                        connection_db.commit()
                        print(f'Awarded social scholarship {social_amt} to {student_name} (ID {student_id})')
                    else:
                        # either insufficient funds or chance failed -> no social scholarship
                        cursor.execute('UPDATE students SET scholarship = 0 WHERE id = ?', (student_id,))
                        connection_db.commit()
                        if remaining < social_amt:
                            print(f'Insufficient funds for social scholarship for {student_name} (ID {student_id}), remaining {remaining}')
                        else:
                            print(f'{student_name} (ID {student_id}) did not pass social scholarship chance (prob={social_prob})')
                        continue  # cannot award academic if social wasn't given

                    # check academic chance: at most two 4s
                    count_fours = sum(1 for g in grade_rows if g == 4)
                    if count_fours <= 2:
                        if random.random() < academic_prob:
                            if remaining >= academic_amt:
                                remaining -= academic_amt
                                awarded_academic += academic_amt
                                student_awarded_total += academic_amt
                                cursor.execute('UPDATE students SET scholarship = ? WHERE id = ?', (student_awarded_total, student_id))
                                connection_db.commit()
                                print(f'Also awarded ACADEMIC scholarship {academic_amt} to {student_name} (ID {student_id})')
                            else:
                                print(f'Insufficient funds for academic scholarship for {student_name} (ID {student_id}), remaining {remaining}')
                        else:
                            print(f'{student_name} (ID {student_id}) did not win academic scholarship by chance')
                    else:
                        print(f'{student_name} (ID {student_id}) has more than two 4s ({count_fours}), not eligible for academic scholarship')

                print('Scholarship distribution finished.')
                print(f'Total social awarded: {awarded_social}, total academic awarded: {awarded_academic}')
                print(f'Remaining scholarship fund: {remaining}')
                
    connection_db.close()