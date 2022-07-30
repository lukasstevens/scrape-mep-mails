
import aiohttp
import asyncio
import logging
import re
from pathlib import Path

from bs4 import BeautifulSoup

LOGGER = logging.getLogger(__name__)
MEPS_FULL_LIST_URL = 'https://www.europarl.europa.eu/meps/en/full-list/all'


async def save_mep_site(
    session: aiohttp.ClientSession,
    directory: Path,
    mep_id: int,
    mep_site_url: str,
) -> None:
    """
    Saves the site of an MEP in *directory*.
    """

    LOGGER.info("Saving MEP site '%d'", mep_id)
    async with session.get(mep_site_url) as mep_site_response:
        assert mep_site_response.status is 200
        with open('./{}/{}.html'.format(directory, mep_id), 'w') as mep_site_file:
            mep_site_file.write(await mep_site_response.text())


async def download_mep_sites(directory: Path, connection_limit: int) -> None:
    """
    Downloads all MEP sites from the EU parliament website to the specified *directory*.
    The *connection_limit* specifies the maximum number of concurrent TCP connections. If the
    limit is too high, the requests may be rate limited / blocked.
    """

    LOGGER.info("Downloading MEP sites to '%s' with connection limit %d", directory, connection_limit)
    session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=connection_limit))

    async with session:
        mep_list_req = await session.get(MEPS_FULL_LIST_URL)
        assert mep_list_req.status is 200

        mep_list_soup = BeautifulSoup(await mep_list_req.text(), 'html.parser')
        mep_sites = []
        for mep_block in mep_list_soup.find_all(id=re.compile('member-block-')):
            mep_block_content = mep_block.find(class_=re.compile('erpl_member-list-item-content'))
            mep_site = mep_block_content['href']

            s = mep_block['id'].split('-')
            assert len(s) is 3
            mep_sites.append((int(s[2]), mep_site))

        LOGGER.info('Found %d MEP sites to download', len(mep_sites))

        tasks = [save_mep_site(session, directory, i, url) for (i, url) in mep_sites]
        await asyncio.gather(*tasks)

    return len(mep_sites)
