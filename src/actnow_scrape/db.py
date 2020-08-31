
import logging
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Iterable

from .scrape import MepData

LOGGER = logging.getLogger(__name__)


def init_db(curs: sqlite3.Cursor) -> None:
    LOGGER.info('Initializing database')
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


def insert_mep_data(curs: sqlite3.Cursor, meps: Iterable[MepData]) -> None:
    country_of_party = {}
    party_id = {party: i for (i, party) in enumerate(set(mep.national_party for mep in meps))}

    for mep in meps:
        country_of_party[mep.national_party] = mep.country
        curs.execute('''
            INSERT INTO meps VALUES (?,?,?,?)
            ''', (mep.id, mep.name, mep.eu_fraction, party_id[mep.national_party]))

        for email in mep.emails:
            curs.execute('''
                INSERT OR IGNORE INTO emails VALUES (?,?)
                ''', (mep.id, email))

        for role in mep.roles:
            for committee in mep.roles[role]:
                curs.execute('''
                    INSERT INTO roles VALUES (?,?,?)
                    ''', (mep.id, role, committee))

    for party, country in country_of_party.items():
        curs.execute('''
            INSERT OR IGNORE INTO national_parties VALUES (?,?,?)
            ''', (party_id[party], party, country))


def save_meps_to_db(db_file: Path, meps: Iterable[MepData]) -> None:
    with sqlite3.connect(db_file) as conn:
        with closing(conn.cursor()) as curs:
            init_db(curs)
            insert_mep_data(curs, meps)
            conn.commit()
