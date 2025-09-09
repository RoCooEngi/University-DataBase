import requests
from requests_ntlm import HttpNtlmAuth
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import os

def url_parser(session: requests.Session, url: str, ssl_certificate='sstu_bundle.pem'):
    '''Makes parsing of url'''
    response = session.get(url, verify=ssl_certificate)
    status = response.status_code
    if status != 200:
        print(f'Page {url} status: {status} - denied.')
        exit(1)
    print(f'Page {url} status: {status} - successful')
    return response

def get_links(response: requests.Response):
    '''Returns a dictionary of links present on the site'''
    soup = BeautifulSoup(response.text, 'html.parser')
    links = {}
    for a in soup.find_all('a', href=True):
        link = a['href']
        if not link.startswith("http"):
            link = requests.compat.urljoin(url, link)
        links[a.get_text(strip=True)] = link
    return links

url = 'https://portal3.sstu.ru/Pages/Default.aspx'
username = 'SSTUEDUDOM\\220123'
password = 'kamelot1'
ssl_certificate = 'sstu_bundle.pem'

session = requests.Session()
session.auth = HttpNtlmAuth(username, password)

# load main page, get required institute (INETM)
response_main = url_parser(session, url)
first_level_links = get_links(response_main)
institute_url = ''
for link in first_level_links.values():
    if 'INETM' in link:
        institute_url = link
        break
if not institute_url:
    print('Recuired institute is not available!')
    exit(1)
print(f'Recuired institute is found: {institute_url}')

# load page of institute, get departments of institute
response_institute = url_parser(session, institute_url)
second_level_links_all = get_links(response_institute)
second_level_links = {}

for name, link in second_level_links_all.items():
    if 'INETM' in link:
        path = urlparse(link).path.strip('/')
        parts = path.split('/')
        if len(parts) == 3 or (len(parts) == 4 and parts[3] == 'default.aspx'):
            second_level_links[name] = link
print(f'Found {len(second_level_links)} faculties')

# iterate for each faculty
for department in second_level_links.items():
    pass





