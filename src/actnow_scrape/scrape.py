
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List

from bs4 import BeautifulSoup

LOGGER = logging.getLogger(__name__)


@dataclass
class MepData:
    id: int
    name: str
    eu_fraction: str
    country: str
    national_party: str
    emails: List[str]
    roles: Dict[str, List[str]]


def scrape_mep_from_html(mep_id: int, html: str) -> MepData:
    """
    Scrape information for an MEP from their EU parliament web site.
    """

    mep_soup = BeautifulSoup(html, 'html.parser')

    name = next(mep_soup.find(class_='sln-member-name').strings).strip()
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
            LOGGER.warning("The email '%s' was dropped because it is malformed", mail)

    statuses = {}
    for status in mep_soup.find_all(class_='erpl_meps-status'):
        status_string = status.find(class_='erpl_title-h4').string
        committes = status.find_all(class_='erpl_committee')
        statuses[status_string] = [c.string for c in committes]

    return MepData(
        id=mep_id,
        name=name,
        eu_fraction=european_fraction,
        country=country,
        national_party=national_party,
        emails=emails,
        roles=statuses,
    )


def scrape_mep_sites(directory: Path) -> Iterable[MepData]:
    """
    Scrapes the MEP data for every HTML page in the specified *directory*.
    """

    for filename in directory.glob('*.html'):
        with open(filename) as fp:
            mep_id = int(filename.stem)
            LOGGER.info('Scraping MEP %d (%s)', mep_id, filename)
            yield scrape_mep_from_html(mep_id, fp.read())
