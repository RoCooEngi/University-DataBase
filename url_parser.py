import sqlite3
import requests
from requests_ntlm import HttpNtlmAuth
from urllib.parse import urlparse
import re
import time
import random
from bs4 import BeautifulSoup
from pprint import pprint
####
# main url
url = 'https://portal3.sstu.ru/Pages/Default.aspx'
# login
USERNAME = 'SSTUEDUDOM\\220123'
PASSWORD = 'kamelot1'
SSL_CERTIFICATE='sstu_bundle.pem'
# which DATABASE table to redact (True to allow)
DB_OPERATION = {
    'all': False,
    'connection': True,
    'institutes': False,
    'departments': False,
    'programs': False,
    'subjects': True
}
session_counter = 0

def create_session():
    '''Makes session'''
    session = requests.Session()
    session.auth = HttpNtlmAuth(USERNAME, PASSWORD)
    return session

def url_parser(session: requests.Session, url: str, SSL_CERTIFICATE=SSL_CERTIFICATE):
    '''Makes parsing of url'''
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
            session = create_session()
            response = url_parser(session, url)
    print(f'Page {url} status: {status} - successful')
    return response

def get_links(response: requests.Response):
    '''Returns a dictionary of links presented on the site'''
    soup = BeautifulSoup(response.text, 'html.parser')
    links = {}
    for a in soup.find_all('a', href=True):
        link = a['href']
        if not link.startswith("http"):
            link = requests.compat.urljoin(url, link)
        links[a.get_text(strip=True)] = link
    return links

def xml_extractor(response: requests.Response):
    '''Finds xml files and returns its url'''
    soup = BeautifulSoup(response.text, 'html.parser')
    for a in soup.find_all(attrs={'o:webquerysourcehref': True}):
        data = a['o:webquerysourcehref']
        if 'XMLDATA' in data:
            return data

def xml_parser(response: requests.Response, key: str):
    '''Parses subjects and its links'''
    soup = BeautifulSoup(response.text, 'lxml-xml')
    return [row.get(key) for row in soup.find_all('z:row') if row.get(key)]
        
def pattern_links(links: dict, pattern: re.Pattern):
    '''Takes out all the links suitable for pattern'''
    return {name: link for name, link in links.items() if re.search(pattern, link)}

def clean_text(t):
    return re.sub(r"\s+", " ", t.strip()) if t else ""

def parse_subject(session: requests.Session, url: str):
    response = session.get(url, verify=SSL_CERTIFICATE)
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

def pause():
    '''Makes pause to avoid recognizong the parsing by server'''
    time.sleep(random.uniform(0.5, 2))

def log_request_error(table: str, reason: int):
    reasons = ('table is not empty', 'permission is missing')
    print(f'Unable to request "{table}": {reasons[reason]}')

def is_icon_td(td):
    '''Checks out if the td is icon (without text, just image or url)'''
    """Проверяет, является ли td иконкой (без текста, только изображения или ссылки)"""
    txt = clean_text(td.get_text(" ", strip=True))
    if txt:
        return False
    imgs = td.find_all("img")
    links = td.find_all("a")
    if imgs and not any(clean_text(a.get_text()) for a in links):
        return True
    return False

def parse_program_page(session: requests.Session, url: str):
    '''Parses subject page, returns normalized data table'''
    response = url_parser(session, url)
    if not response:
        return None
    soup = BeautifulSoup(response.text, "html.parser")
    tables = soup.find_all("table", class_="ms-listviewtable")
    all_parsed = []

    for table in tables:
        # wind headers
        header_tr = table.find("tr", class_=re.compile(r"ms-viewheadertr|ms-headerrow|ms-viewheader"))
        headers = []
        for h in table.find_all(class_=re.compile(r"ms-vh")):
            t = clean_text(h.get_text(" ", strip=True))
            if t and t not in headers:
                headers.append(t)
        if not headers and header_tr:
            headers = [clean_text(x.get_text(" ", strip=True)) for x in header_tr.find_all(['th', 'td']) if clean_text(x.get_text(" ", strip=True))]

        # collect rows
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

    # Choose the table with 'семестр' или 'курс'
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

# creating a database (if not yet)
connection_db = sqlite3.connect('university.db')
cursor = connection_db.cursor()
with open('tables_init.sql', 'r', encoding='utf-8') as file:
    cursor.executescript(file.read())
connection_db.commit()

# connection to the link
if DB_OPERATION['connection'] or DB_OPERATION['all']:
    session = create_session()

# load main page, get all institues
if cursor.execute('SELECT COUNT(*) FROM institutes').fetchone()[0] != 0:
    log_request_error('institutes', 0)
elif not (DB_OPERATION['institutes'] or DB_OPERATION['all']):
    log_request_error('institutes', 1)
else:
    response_main = url_parser(session, url)
    first_level_links = get_links(response_main)
    first_level_pattern = re.compile(r"Facult/[A-Z]+(?=/|$)")
    links_inst = pattern_links(first_level_links, first_level_pattern)
    if not links_inst:
        print('Dict of institutes is empty! Check the url parser')
        exit(1)
    cursor.executemany('INSERT INTO institutes (name, url) VALUES (?, ?)', tuple(links_inst.items()))
    connection_db.commit()
    print('Institutes data has been saved')
    pause()

# load page of institute, get departments of institute
if cursor.execute('SELECT COUNT(*) FROM departments').fetchone()[0] != 0:
    log_request_error('departments', 0)
elif not (DB_OPERATION['departments'] or DB_OPERATION['all']):
    log_request_error('departments', 1)
else:
    cursor.execute('SELECT id, url FROM institutes ORDER BY id')
    institutes = cursor.fetchall()
    for inst_id, inst_url in institutes:
        response_inst = url_parser(session, inst_url)
        if not response_inst:
            continue
        second_level_links = get_links(response_inst)
        second_level_pattern = re.compile(r"/Facult/[A-Z]+/[A-Z]+(?:/default\.aspx)?$")
        links_dep = pattern_links(second_level_links, second_level_pattern)
        if not links_dep:
            print('Dict of departments is empty! Check the url parser')
        else:
            cursor.executemany('INSERT INTO departments (name, url, institute_id) VALUES (?, ?, ?)',
                            [i + (inst_id,) for i in tuple(links_dep.items())])
        connection_db.commit()
        pause()
    print('Departments data has been saved')

# load page of department, get programs of department
if cursor.execute('SELECT COUNT(*) FROM programs').fetchone()[0] != 0:
    log_request_error('programs', 0)
elif not (DB_OPERATION['programs'] or DB_OPERATION['all']):
    log_request_error('programs', 1)
else:
    cursor.execute('SELECT id, url FROM departments ORDER BY id')
    departments = cursor.fetchall()
    for dep_id, dep_url in departments:
        response_dep = url_parser(session, dep_url)
        if not response_dep:
            continue
        third_level_links = get_links(response_dep)
        third_level_pattern = re.compile(r"/\d{2}\.\d{2}\.\d{2}[^/]*(?:/default\.aspx)?$")
        links_prog = pattern_links(third_level_links, third_level_pattern)
        if not links_prog:
            print('Dict of programs is empty! Check the url parser')
        else:
            cursor.executemany('INSERT INTO programs (name, url, department_id) VALUES (?, ?, ?)',
                            [i + (dep_id,) for i in tuple(links_prog.items())])
        connection_db.commit()
        pause()
    print('Programs data has been saved')

# load page of programs, get all subjects of program
if cursor.execute('SELECT COUNT(*) FROM subjects').fetchone()[0] != 0 and not (DB_OPERATION['subjects'] or DB_OPERATION['all']):
    log_request_error('subjects', 1)
else:
    # Get the last subject to determine where to resume
    cursor.execute('SELECT id, name, program_id FROM subjects ORDER BY id DESC LIMIT 1')
    last_subject = cursor.fetchone()
    last_subject_id = last_subject[0] if last_subject else 0
    last_program_id = last_subject[2] if last_subject else 0
    last_subject_name = last_subject[1] if last_subject else ""

    cursor.execute('SELECT id, url FROM programs WHERE id >= ? ORDER BY id', (last_program_id,))
    programs = cursor.fetchall()
    
    for prog_id, prog_url in programs:
        response_prog = url_parser(session, prog_url)
        if not response_prog:
            continue
        xml_link = xml_extractor(response_prog)
        response_xml = url_parser(session, xml_link)
        if not response_xml:
            continue
        subjects = xml_parser(response_xml, 'ows__x041d__x0430__x0438__x043c__x04')
        subjects = {sub.strip(' /'): link.strip() for raw_value in subjects for link, sub in [raw_value.split(',', 1)]}

        # Skip subjects before the last known subject for the current program
        start_processing = False if prog_id == last_program_id else True
        for sub_name, sub_url in subjects.items():
            # If in the same program as the last subject, skip until the last subject name
            if prog_id == last_program_id and not start_processing:
                if sub_name == last_subject_name:
                    start_processing = True
                continue

            try:
                # Parse subject page
                norm_rows = parse_program_page(session, sub_url)
                semester = 0
                for row in norm_rows:
                    if row.get("Семестр"):
                        try:
                            semester = int(row["Семестр"])
                        except (ValueError, TypeError):
                            print(f"Invalid semester value for {sub_name}: {row['Семестр']}")
                            semester = 0
                        break

                # Save to DB
                cursor.execute('INSERT INTO subjects (name, semester, program_id) VALUES (?, ?, ?)',
                              (sub_name, semester, prog_id))
                connection_db.commit()
                print(f'Subject: {sub_name}, Semester: {semester}, Program id: {prog_id}')
            except Exception as e:
                print(f"Error parsing subject {sub_name} at {sub_url}: {str(e)}")
                semester = 0
                cursor.execute('INSERT INTO subjects (name, semester, program_id) VALUES (?, ?, ?)',
                              (sub_name, semester, prog_id))
                connection_db.commit()
                print(f'Subject: {sub_name}, Semester: {semester} (set to 0 due to error), Program id: {prog_id}')
            pause()
        pause()

    print('Subjects data has been saved')





