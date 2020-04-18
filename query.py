#!/usr/bin/env python3
import asyncio
from bs4 import BeautifulSoup
from pathlib import Path
import re
import requests
import sqlite3

MEP_SITE_FOLDER = './mep_sites/'

async def download_mep_sites():
    mep_list_req = requests.get('https://www.europarl.europa.eu/meps/en/full-list/all')
    assert mep_list_req.status_code is 200

    mep_list_soup = BeautifulSoup(mep_list_req.text, 'html.parser')

    mep_sites = []
    for mep_block in mep_list_soup.find_all(id=re.compile('member-block-')):
        mep_block_content = mep_block.find(class_=re.compile('erpl_member-list-item-content'))
        mep_site = mep_block_content['href']

        s = mep_block['id'].split('-')
        assert len(s) is 3
        mep_sites.append((s[2], mep_site))


    Path(MEP_SITE_FOLDER).mkdir(parents=True, exist_ok=True)

    async def save_mep_site(mep_id, mep_site_url):
        mep_site_req = requests.get(mep_site_url)
        assert mep_site_req.status_code is 200

        with open('./{}/{}.html'.format(MEP_SITE_FOLDER, mep_id), 'w') as mep_site_file:
            mep_site_file.write(mep_site_req.text)


    tasks = [save_mep_site(i, url) for (i, url) in mep_sites]
    results = await asyncio.gather(*tasks)


async def scrape(mep_site_path):
    with open(mep_site_path, 'r') as site_file:
        mep_site_content = site_file.read()
        mep_soup = BeautifulSoup(mep_site_content, 'html.parser')

        name = next(mep_soup.find(class_='erpl_title-h1 mt-1').strings).strip()
        european_fraction = mep_soup.find(class_='erpl_title-h3 mt-1').string

        national_info_tag = mep_soup.find(class_='erpl_title-h3 mt-1 mb-1')
        national_info = national_info_tag.string.split(' - ')
        nation = national_info[0].strip()
        national_party = national_info[-1].strip()

        def descramble(mail):
            mail = mail.replace('[dot]', '.').replace('[at]', '@').replace('mailto:', '')
            return mail[::-1]

        emails = [descramble(a['href']) for a in mep_soup.find_all(class_=re.compile('link_email'))]


        statuses = {}
        for status in mep_soup.find_all(class_='erpl_meps-status'):
            status_string = status.find(class_='erpl_title-h4').string
            committes = status.find_all(class_='erpl_committee')
            statuses[status_string] = [c.string for c in committes]

        return {'id': Path(mep_site_path).stem, 'name': name, 'eu_fraction': european_fraction, 'nation': nation,
                'national_party': national_party, 'emails': emails, 'roles': statuses}

async def scrape_all():
    path = Path(MEP_SITE_FOLDER)
    statuses = set()

    files = list(path.glob('*.html'))
    tasks = [scrape(f) for f in files]
    results = await asyncio.gather(*tasks)

    return results

def gen_mailto_link(meps):
    link = 'mailto:' + ','.join([mep['emails'][0] for mep in meps])
    return link

def init_db(conn):
    curs = conn.cursor()

    curs.execute('''
        CREATE TABLE meps
            (mep_id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL,
            nation TEXT, eu_fraction TEXT, national_party TEXT)
        ''')

    curs.execute('''
        CREATE TABLE emails
            (mep_id INTEGER, email TEXT NOT NULL, FOREIGN KEY (mep_id) REFERENCES meps(mep_id))
        ''')

    curs.execute('''
        CREATE TABLE roles
            (mep_id INTEGER, committee TEXT NOT NULL, role TEXT,
            FOREIGN KEY (mep_id) REFERENCES meps(mep_id))
        ''')

    conn.commit()

def save_to_db(meps, db='meps.db'):
    conn = sqlite3.connect(db)
    curs = conn.cursor()

    init_db(conn)

    for mep in meps:
        curs.execute('''
            INSERT INTO meps VALUES
                (?,?,?,?,?)
            ''', (int(mep['id']), mep['name'], mep['nation'], mep['eu_fraction'], mep['national_party']))

        for email in mep['emails']:
            curs.execute('''
                INSERT INTO emails VALUES (?,?)
                ''', (int(mep['id']), email))

        for role in mep['roles']:
            for committee in mep['roles'][role]:
                curs.execute('''
                    INSERT INTO roles VALUES (?,?,?)
                    ''', (int(mep['id']), role, committee))

    conn.commit()
    conn.close()

if __name__ == '__main__':
    #asyncio.run(download_mep_sites())
    loop = asyncio.get_event_loop()
    res = loop.run_until_complete(loop.create_task(scrape_all()))
    save_to_db(res)
    #print(gen_mailto_link([mep for mep in res if mep['nation'] == 'Germany']))

