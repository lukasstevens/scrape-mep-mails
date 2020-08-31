import asyncio
import click
import json
import logging
import shutil
import sqlite3
from pathlib import Path

from .db import save_meps_to_db
from .download import download_mep_sites
from .scrape import scrape_mep_sites

LOGGER = logging.getLogger(__name__)


@click.group()
@click.option('-v', '--verbose', count=True, help='increase log verbosity.')
@click.option('-q', '--quiet', count=True, help='decrease log verbosity.')
def cli(verbose, quiet):
    """
    Scrape data from the members of the European Parliament (MEPs).
    """

    log_level = {
        -2: logging.CRITICAL,
        -1: logging.ERROR,
        0: logging.WARNING,
        1: logging.INFO,
        2: logging.DEBUG,
    }[max(-2, min(2, verbose - quiet))]

    logging.basicConfig(format='[%(levelname)s: %(asctime)s]: %(message)s', level=log_level)


@cli.command()
@click.option('--output-dir', '-o', default='mep_sites/', type=Path, help='output directory')
@click.option('--force', '-f', is_flag=True, help='overwrite the output directory')
@click.option('--connection-limit', '-l', type=int, default=10,
    help='the maximum number of concurrent TCP connections. If the limit is too high, '
         'requests may be blocked (default: %(default)s)')
def download(output_dir, force, connection_limit):
    """
    Download the websites of the MEPs to a local directory.
    """

    if output_dir.exists() and not force:
        print('The path {} already exists. Consider using the --force option.'.format(str(output_dir)))
    else:
        if output_dir.exists():
            shutil.rmtree(output_dir)
        output_dir.mkdir()
        asyncio.run(download_mep_sites(output_dir, connection_limit))


@cli.command()
@click.option('--input-dir', '-i', type=Path, default='mep_sites/', help='input directory')
@click.option('--output-db', '-o', type=Path, default='meps.db',
    help='filename of the SQLite3 database (default: %(default)s)')
@click.option('--force', '-f', is_flag=True, help='overwrite the output database')
def initdb(input_dir, output_db, force):
    """
    Scrape the MEP websites previously downloaded and populate an SQLite3 database with the data.
    """

    if not input_dir.is_dir():
        print('The input directory {} does not exist.'.format(str(input_dir)))
        return
    if output_db.exists() and not force:
        print('The path {} already exists. Consider using the --force option.'.format(str(output_db)))
    else:
        if output_db.exists():
            output_db.unlink()
        meps = scrape_mep_sites(input_dir)
        save_meps_to_db(output_db, meps)


@cli.command()
@click.option('--input-db', '-i', type=Path, default='meps.db', help='input database (default: %(default)s)')
def dumpschema(input_db):
    """
    Dump the schema of an SQLite3 database.
    """

    conn = sqlite3.connect(input_db)
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
            (_cid, field_name, typ, notnull, dflt_value, pk) = column
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
    cli()  # pylint: disable=no-value-for-parameter
