import sqlite3
import requests
from requests_ntlm import HttpNtlmAuth
from urllib.parse import urlparse
import re
import time
import random
from bs4 import BeautifulSoup

url = 'https://portal3.sstu.ru/Pages/Default.aspx'
username = 'SSTUEDUDOM\\220123'
password = 'kamelot1'
ssl_certificate = 'sstu_bundle.pem'

def url_parser(session: requests.Session, url: str, ssl_certificate='sstu_bundle.pem'):
    '''Makes parsing of url'''
    response = session.get(url, verify=ssl_certificate)
    status = response.status_code
    if status != 200:
        print(f'Page {url} status: {status} - denied.')
        if status == 401:
            print('Trying to reconnect...')
            session = requests.Session()
            session.auth = HttpNtlmAuth(username, password)
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

def pattern_links(links: dict, pattern: re.Pattern):
    '''Takes out all the links suitable for pattern'''
    return {name: link for name, link in links.items() if re.search(pattern, link)}

def pause():
    '''Makes pause to avoid recognizong the parsing by server'''
    time.sleep(random.uniform(1, 3))

# creating a database (if not yet)
connection_db = sqlite3.connect('university.db')
cursor = connection_db.cursor()
with open('tables_init.sql', 'r', encoding='utf-8') as file:
    cursor.executescript(file.read())
connection_db.commit()

# connection to the link
connection = True
if connection:
    session = requests.Session()
    session.auth = HttpNtlmAuth(username, password)

# load main page, get all institues
if cursor.execute('SELECT COUNT(*) FROM institutes').fetchone()[0] == 0 and connection:
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
else:
    print('Unable to request "institutes" table (either it is not empty or permission is missing)')

# load page of institute, get departments of institute
if cursor.execute('SELECT COUNT(*) FROM departments').fetchone()[0] == 0 and connection:
    cursor.execute('SELECT id, url FROM institutes ORDER BY id')
    institutes = cursor.fetchall()
    for institute in institutes:
        inst_id, inst_url = institute
        response_inst = url_parser(session, inst_url)
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
else:
    print('Unable to request "departments" table (either it is not empty or permission is missing)')

# load page of programs
if cursor.execute('SELECT COUNT(*) FROM programs').fetchone()[0] == 0 and connection:
    cursor.execute('SELECT id, url FROM departments ORDER BY id')
    departments = cursor.fetchall()
    for department in departments:
        dep_id, dep_url = department
        response_dep = url_parser(session, dep_url)
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
else:
    print('Unable to request "programs" table (either it is not empty or permission is missing)')

connection_db.close()





