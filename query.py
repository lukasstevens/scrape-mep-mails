#!/usr/bin/env python3
import aiohttp
import argparse
import asyncio
from bs4 import BeautifulSoup
from pathlib import Path
import re
import requests
import shutil
import sys
import sqlite3
import json

async def download_mep_sites(path, connection_limit):
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=connection_limit)) as session:
        mep_list_req = await session.get('https://www.europarl.europa.eu/meps/en/full-list/all')
        assert mep_list_req.status is 200

        mep_list_soup = BeautifulSoup(await mep_list_req.text(), 'html.parser')

        mep_sites = []
        for mep_block in mep_list_soup.find_all(id=re.compile('member-block-')):
            mep_block_content = mep_block.find(class_=re.compile('erpl_member-list-item-content'))
            mep_site = mep_block_content['href']

            s = mep_block['id'].split('-')
            assert len(s) is 3
            mep_sites.append((s[2], mep_site))


        async def save_mep_site(mep_id, mep_site_url):
            async with session.get(mep_site_url) as mep_site_response:
                assert mep_site_response.status is 200

                with open('./{}/{}.html'.format(path, mep_id), 'w') as mep_site_file:
                    mep_site_file.write(await mep_site_response.text())


        tasks = [save_mep_site(i, url) for (i, url) in mep_sites]
        results = await asyncio.gather(*tasks)


def scrape(mep_site_path):
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

        emails = []
        for a in mep_soup.find_all(class_=re.compile('link_email')):
            mail = descramble(a['href'])
            if '@' in mail:
                emails.append(mail)
            else:
                print('The email {} was dropped since it is malformed'.format(mail), file=sys.stderr)

        statuses = {}
        for status in mep_soup.find_all(class_='erpl_meps-status'):
            status_string = status.find(class_='erpl_title-h4').string
            committes = status.find_all(class_='erpl_committee')
            statuses[status_string] = [c.string for c in committes]

        return {'id': int(Path(mep_site_path).stem), 'name': name, 'eu_fraction': european_fraction, 'country': country,
                'national_party': national_party, 'emails': emails, 'roles': statuses}

async def scrape_all(path):
    files = list(path.glob('*.html'))
    return [scrape(f) for f in files]

def gen_mailto_link(meps):
    link = 'mailto:' + ','.join([mep['emails'][0] for mep in meps])
    return link

def init_db(conn):
    curs = conn.cursor()

    curs.execute('''
        CREATE TABLE national_parties
            (national_party_id INTEGER NOT NULL PRIMARY KEY, party TEXT NOT NULL, country TEXT NOT NULL,
            CONSTRAINT Unational_parties UNIQUE(party, country))
            ''')

    curs.execute('''
        CREATE TABLE meps
            (mep_id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL,
            eu_fraction TEXT NOT NULL, national_party_id INTEGER NOT NULL,
            FOREIGN KEY (national_party_id) REFERENCES national_parties(national_party_id))
        ''')

    curs.execute('''
        CREATE TABLE emails
            (mep_id INTEGER, email TEXT NOT NULL,
            CONSTRAINT Uemails UNIQUE(mep_id, email),
            FOREIGN KEY (mep_id) REFERENCES meps(mep_id))
        ''')

    curs.execute('''
        CREATE TABLE roles
            (mep_id INTEGER, role TEXT NOT NULL, committee TEXT NOT NULL,
            FOREIGN KEY (mep_id) REFERENCES meps(mep_id))
        ''')

    conn.commit()

def save_to_db(meps, db):
    conn = sqlite3.connect(db)
    curs = conn.cursor()

    init_db(conn)

    country_of_party = {}
    party_id = {party: i for (i, party) in enumerate(set(mep['national_party'] for mep in meps))}

    for mep in meps:
        country_of_party[mep['national_party']] = mep['country']
        curs.execute('''
            INSERT INTO meps VALUES (?,?,?,?)
            ''', (mep['id'], mep['name'], mep['eu_fraction'], party_id[mep['national_party']]))

        for email in mep['emails']:
            curs.execute('''
                INSERT OR IGNORE INTO emails VALUES (?,?)
                ''', (mep['id'], email))

        for role in mep['roles']:
            for committee in mep['roles'][role]:
                curs.execute('''
                    INSERT INTO roles VALUES (?,?,?)
                    ''', (mep['id'], role, committee))

    for party, country in country_of_party.items():
        curs.execute('''
            INSERT OR IGNORE INTO national_parties VALUES (?,?,?)
            ''', (party_id[party], party, country))

    conn.commit()

    curs.close()
    conn.close()


def download(args):
    p = Path(args.output_dir)
    if p.exists() and not args.force:
        print('The path {} already exists. Consider using the --force option.'.format(str(p)))
    else:
        if p.exists():
            shutil.rmtree(p)
        p.mkdir()
        asyncio.run(download_mep_sites(p, args.connection_limit))

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

def dumpschema(args):
    conn = sqlite3.connect(Path(args.input_db))
    curs = conn.cursor()

    curs.execute("SELECT tbl_name FROM sqlite_master WHERE type='table';")
    tables = curs.fetchall()

    schema = {}
    for (table,) in tables:
        meta = curs.execute("PRAGMA table_info('{}')".format(table))
        schema[table] = {}
        schema[table]['Name'] = table
        schema[table]['Columns'] = {}
        for column in meta:
            (cid, field_name, typ, notnull, dflt_value, pk) = column
            schema[table]['Columns'][field_name] = {
                'Name': field_name,
                'PrimaryKey': bool(pk),
                'Type': typ,
                'Null': not bool(notnull),
                'Default': dflt_value
                }

    print(json.dumps(schema, indent=2))

    curs.close()
    conn.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title='subcommands', dest='cmd')
    subparsers.required = True

    parser_download = subparsers.add_parser('download', aliases=['dl'], help='Download the websites of the MEPs.')
    parser_download.set_defaults(func=download)
    parser_download.add_argument('--output_dir', '-o', type=str, default='mep_sites/',
            help='the output directory for the scraped MEP sites (default: %(default)s)')
    parser_download.add_argument('--force', '-f', action='store_true',
            help='a flag indicating whether the output_dir should be overwritten')
    parser_download.add_argument('--connection_limit', '-l', type=int, default=10,
            help='the number of concurrent tcp connections when scraping. If the limit is too high, the site might block requests. (default %(default)s)')


    parser_initdb = subparsers.add_parser('initdb', help='Create and populate a SQLite3 database with the scraped data')
    parser_initdb.set_defaults(func=initdb)
    parser_initdb.add_argument('--input_dir', '-i', type=str, default='mep_sites/',
            help='the directory of the scraped MEP websites (default: %(default)s)')
    parser_initdb.add_argument('--output', '-o', type=str, default='meps.db',
            help='filename of the SQLite3 database (default: %(default)s)')
    parser_initdb.add_argument('--force', '-f', action='store_true',
            help='a flag indicating whether the database should be overwritten')

    parser_dumpschema = subparsers.add_parser('dumpschema', help='Dump the schema of a SQLite3 database')
    parser_dumpschema.set_defaults(func=dumpschema)
    parser_dumpschema.add_argument('--input_db', '-i', type=str, default='meps.db',
            help='the input database (default: %(default)s)')

    args = parser.parse_args(sys.argv[1:])
    args.func(args)

