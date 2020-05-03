#!/usr/bin/env python3
import argparse
import asyncio
from bs4 import BeautifulSoup
from pathlib import Path
import re
import requests
import shutil
import sys
import sqlite3

async def download_mep_sites(path):
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


    async def save_mep_site(mep_id, mep_site_url):
        mep_site_req = requests.get(mep_site_url)
        assert mep_site_req.status_code is 200

        with open('./{}/{}.html'.format(path, mep_id), 'w') as mep_site_file:
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
        country = national_info[0].strip()
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

        return {'id': int(Path(mep_site_path).stem), 'name': name, 'eu_fraction': european_fraction, 'country': country,
                'national_party': national_party, 'emails': emails, 'roles': statuses}

async def scrape_all(path):
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
            eu_fraction TEXT NOT NULL, national_party TEXT NOT NULL)
        ''')

    curs.execute('''
        CREATE TABLE emails
            (mep_id INTEGER, email TEXT NOT NULL, FOREIGN KEY (mep_id) REFERENCES meps(mep_id))
        ''')

    curs.execute('''
        CREATE TABLE roles
            (mep_id INTEGER, committee TEXT NOT NULL, role TEXT NOT NULL,
            FOREIGN KEY (mep_id) REFERENCES meps(mep_id))
        ''')

    curs.execute('''
        CREATE TABLE national_parties
            (party TEXT NOT NULL, country TEXT NOT NULL)
            ''')

    conn.commit()

def save_to_db(meps, db):
    conn = sqlite3.connect(db)
    curs = conn.cursor()

    init_db(conn)

    country_of_party = {}
    for mep in meps:
        country_of_party[mep['national_party']] = mep['country']
        curs.execute('''
            INSERT INTO meps VALUES (?,?,?,?)
            ''', (mep['id'], mep['name'], mep['eu_fraction'], mep['national_party']))

        for email in mep['emails']:
            curs.execute('''
                INSERT INTO emails VALUES (?,?)
                ''', (mep['id'], email))

        for role in mep['roles']:
            for committee in mep['roles'][role]:
                curs.execute('''
                    INSERT INTO roles VALUES (?,?,?)
                    ''', (mep['id'], role, committee))

    for party, country in country_of_party.items():
        curs.execute('''
            INSERT INTO national_parties VALUES (?,?)
            ''', (party, country))

    conn.commit()
    conn.close()


def download(args):
    p = Path(args.output_dir)
    if p.exists() and not args.force:
        print('The path {} already exists. Consider using the --force option.'.format(str(p)))
    else:
        if p.exists():
            shutil.rmtree(p)
        p.mkdir()
        asyncio.run(download_mep_sites(p))

def initdb(args):
    input_path = Path(args.input_dir)
    output_path = Path(args.output)
    if not input_path.is_dir():
        print('The input directory {} does not exist.'.format(str(input_path)))
        return
    if output_path.exists() and not args.force:
        print('The path {} already exists. Consider using the --force option.'.format(str(output_path)))
    else:
        if output_path.exists():
            output_path.unlink()
        loop = asyncio.get_event_loop()
        meps = loop.run_until_complete(loop.create_task(scrape_all(input_path)))
        save_to_db(meps, output_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title='subcommands', dest='cmd')
    subparsers.required = True

    parser_download = subparsers.add_parser('download', aliases=['dl'], help='Download the websites of the MEPs.')
    parser_download.set_defaults(func=download)
    parser_download.add_argument('--output_dir', '-o', type=str, default='mep_sites/',
            help='the output directory for the scraped MEP sites (default: %(default)s)')
    parser_download.add_argument('--force', '-f', action='store_true',
            help='a flag indicating whether the output-dir should be overwritten')


    parser_initdb = subparsers.add_parser('initdb', help='Create and populate a SQLite3 database with the scraped data')
    parser_initdb.set_defaults(func=initdb)
    parser_initdb.add_argument('--input_dir', '-i', type=str, default='mep_sites/',
            help='The directory of the scraped MEP websites (default: %(default)s)')
    parser_initdb.add_argument('--output', '-o', type=str, default='meps.db',
            help='Filename of the SQLite3 database (default: %(default)s)')
    parser_initdb.add_argument('--force', '-f', action='store_true',
            help='a flag indicating whether the database should be overwritten')

    args = parser.parse_args(sys.argv[1:])
    args.func(args)

