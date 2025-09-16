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
        'students generator': False,
    }
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
    
    connection_db.close()
    