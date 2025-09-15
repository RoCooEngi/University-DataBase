import sqlite3
import requests
from requests_ntlm import HttpNtlmAuth
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import re
import time
import random
from pprint import pprint

# program configuration
CONFIG = {
    'MAIN_URL': 'https://portal3.sstu.ru/Pages/Default.aspx',
    'USERNAME': 'SSTUEDUDOM\\220123',
    'PASSWORD': 'kamelot1',
    'SSL_CERTIFICATE': 'sstu_bundle.pem',
    'DB_NAME': 'university.db',
    'DB_OPERATIONS': {
        'all': False,
        'connection': False,
        'institutes': False,
        'departments': False,
        'programs': False,
        'subjects': False,
        'semester correction': False,
    }
}
session_counter = 0

def create_session():
    '''Makes session'''
    session = requests.Session()
    session.auth = HttpNtlmAuth(CONFIG['USERNAME'], CONFIG['PASSWORD'])
    return session

def url_parser(session: requests.Session, url: str, SSL_CERTIFICATE=CONFIG['SSL_CERTIFICATE']):
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
            link = requests.compat.urljoin(CONFIG['MAIN_URL'], link)
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

def pause():
    '''Makes pause to avoid recognizong the parsing by server'''
    time.sleep(random.uniform(0.5, 2))

def log_request_error(table: str):
    print(f'Unable to request "{table}": permission is missing')

def is_icon_td(td):
    '''Checks out if the td is icon (without text, just image or url)'''
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
if CONFIG['DB_OPERATIONS']['connection'] or CONFIG['DB_OPERATIONS']['all']:
    session = create_session()

# load main page, get all institutes
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
    
    # Get existing institutes
    cursor.execute('SELECT name, url FROM institutes')
    existing_institutes = dict(cursor.fetchall())
    
    # Filter new institutes
    new_institutes = [(name, url) for name, url in links_inst.items() if name not in existing_institutes or existing_institutes[name] != url]
    if new_institutes:
        cursor.executemany('INSERT OR REPLACE INTO institutes (name, url) VALUES (?, ?)', new_institutes)
        connection_db.commit()
        print(f'{len(new_institutes)} new or updated institutes have been saved')
    else:
        print('No new or updated institutes to save')
    pause()

# load page of institute, get departments of institute
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

# load page of department, get programs of department
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

# load page of programs, get all subjects of program
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
        
        # Check existing subjects for this program
        cursor.execute('SELECT name, semester FROM subjects WHERE program_id = ?', (prog_id,))
        existing_subjects = {(name, semester) for name, semester in cursor.fetchall()}
        
        new_subjects = []
        for sub_name, sub_url in subjects.items():
            semester = 0
            try:
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
            except Exception as e:
                print(f"Error parsing subject {sub_name} at {sub_url}: {str(e)}")
                semester = 0
            # Check if subject already exists with the same semester
            if (sub_name, semester) not in existing_subjects:
                new_subjects.append((sub_name, semester, prog_id))
                print(f'Subject: {sub_name}, Semester: {semester}, Program id: {prog_id}')
                
        if new_subjects:
            cursor.executemany('INSERT INTO subjects (name, semester, program_id) VALUES (?, ?, ?)', new_subjects)
            connection_db.commit()
            print(f'{len(new_subjects)} new subjects saved for program {prog_id}')
        else:
            print(f'No new subjects for program {prog_id}')
        pause()
    print('Subjects data has been saved')

# makes updates for zero data subjects' semesters
if not (CONFIG['DB_OPERATIONS']['semester correction'] or CONFIG['DB_OPERATIONS']['all']):
    print('Unable to update semesters: permission is missing')
else:
    cursor.execute('SELECT * FROM subjects WHERE semester = 0')
    zero_semester_rows = cursor.fetchall()
    
    if zero_semester_rows:
        for sub_id, sub_name, sub_sem, prog_id in zero_semester_rows:
            cursor.execute(f'SELECT name FROM programs WHERE id = {prog_id}')
            prog_name = cursor.fetchall()[0][0].lower()
            
            if 'магистр' in prog_name:
                new_sem = random.randint(1, 4)
            elif 'бакалавр' in prog_name:
                new_sem = random.randint(1, 8)
            else:
                new_sem = random.randint(1, 10)
            cursor.execute(f'UPDATE subjects SET semester = {new_sem} WHERE id = {sub_id}')
            print(f'{sub_name} semester was set to {new_sem} ({prog_name})')
        connection_db.commit()
        print('Semesters data has been updated!')
    else:
        print('No semester data to update')

# fills students with data

connection_db.close()