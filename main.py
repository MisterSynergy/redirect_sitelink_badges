from collections.abc import Generator
from io import StringIO
import logging
import logging.config
from os.path import expanduser
from time import sleep, strftime
from typing import Any, Optional

import pandas as pd
import pywikibot as pwb
from pywikibot.exceptions import NoPageError, OtherPageSaveError, IsRedirectPageError, CircularRedirectError, \
    InterwikiRedirectPageError, APIError, CascadeLockedPageError, LockedPageError, NoUsernameError, \
    TitleblacklistError, UnknownSiteError
import requests
import mariadb

logging.config.fileConfig('logging.conf')
LOG = logging.getLogger()

WDQS_ENDPOINT = 'https://query.wikidata.org/sparql'
WDQS_USER_AGENT = f'{requests.utils.default_headers()["User-Agent"]} (Wikidata bot' \
                   ' by User:MisterSynergy; mailto:mister.synergy@yahoo.com)'

SITE = pwb.Site('wikidata', 'wikidata')
REPO = SITE.data_repository()

QID_S2R = 'Q70893996'
QID_I2R = 'Q70894304'

REPORT_UNCONNECTED_TARGET = 'Wikidata:Database reports/Sitelink to redirect with unconnected target'

REDIRECT_LENGTH_CUTOFF = 100  # bytes; longer redirect pages are treated as valid even if the target does not exist
EDIT_SUMMARY_APPENDIX:str = ' #msynbotTask10'

PROCESS_MISSING_S2R_BADGE = True
PROCESS_BOTH_BADGE_SITUATIONS = True
PROCESS_NON_REDIRECTS = True
PROCESS_INEXISTENT_TARGETS = True
PROCESS_UNCONNECTED_TARGETS = True
SIMULATE = False

FAMILY_SHORTCUTS = {
    'commonswiki' : 'c:',
    'mediawikiwiki' : 'mw:',
    'metawiki' : 'm:',
    'specieswiki' : 'species:',
    'simplewiki' : 'w:simple:',
    'wikibooks' : 'b:',
    'wikinews' : 'n:',
    'wikipedia' : 'w:',
    'wikiquote' : 'q:',
    'wikisource' : 's:',
    'wikiversity' : 'v:',
    'wikivoyage' : 'voy:',
    'wiktionary' : 'wikt:',
}


class Replica:
    def __init__(self, database:str):
        params = {
            'host' : f'{database}.analytics.db.svc.wikimedia.cloud',
            'database' : f'{database}_p',
            'default_file' : f'{expanduser("~")}/replica.my.cnf'
        }
        self.replica = mariadb.connect(**params)
        self.cursor = self.replica.cursor(dictionary=True)

    def __enter__(self):
        return self.cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        self.replica.close()


def query_mediawiki(database:str, query:str, params:Optional[tuple[Any]]=None) -> Generator[dict[str, Any], None, None]:
    with Replica(database) as db_cursor:
        if params is None:
            db_cursor.execute(query)
        else:
            db_cursor.execute(query, params)

        result = db_cursor.fetchall()

        for row in result:
            yield row


def query_mediawiki_to_dataframe(database:str, query:str) -> pd.DataFrame:
    with Replica(database) as db_cursor:
        db_cursor.execute(query)
        result = db_cursor.fetchall()

    df = pd.DataFrame(data=result)

    return df


def query_wdqs_to_dataframe(query:str, columns:dict[str, Any]) -> pd.DataFrame:
    df = pd.read_csv(
        StringIO(
            requests.post(
                url=WDQS_ENDPOINT,
                data={ 'query' : query },
                headers={
                    'User-Agent': WDQS_USER_AGENT,
                    'Accept' : 'text/csv'
                }
            ).text
        ),
        header=0,
        names=list(columns.keys()),
        dtype=columns
    )
    return df


def clear_logfiles() -> None:
    for logfile in [ './output/cases.tsv', './output/project_stats.tsv', './output/unconnected_wikitable_body.txt' ]:
        open(logfile, mode='w', encoding='utf8').write('')


def query_database_names() -> list[dict[str, str]]:
    # as in https://quarry.wmcloud.org/query/12744
    query = """SELECT dbname, url, family, lang FROM wiki WHERE is_closed=0 AND has_wikidata=1"""
    db_names = []
    for row in query_mediawiki('meta', query):
        payload = {
            'db_name' : row['dbname'],
            'url' : row['url'],
            'family' : row['family'],
            'language' : row['lang']
        }
        db_names.append(payload)

    return db_names


def query_namespaces_from_api(url:str) -> dict[int, dict[str, str]]:
    ns = {}

    response = requests.get(
        url=f'{url}/w/api.php',
        params={
            'action' : 'query',
            'meta' : 'siteinfo',
            'siprop' : 'namespaces',
            'format' : 'json'
        },
        headers={
            'User-Agent': WDQS_USER_AGENT,
        }
    )

    payload = response.json()
    for dct in payload.get('query', {}).get('namespaces', {}).values():
        ns[dct.get('id', -3)] = { 'local' : dct.get('*', ''), 'canonical' : dct.get('canonical', '') }

    return ns


def query_redirect_pages_linked_to_wikidata_item(database:str='enwiki') -> pd.DataFrame:
    query = """SELECT
  CONVERT(redirect_page.page_id USING utf8) AS redirect_id,
  redirect_page.page_namespace AS redirect_namespace,
  CONVERT(redirect_page.page_title USING utf8) AS redirect_title,
  CONVERT(redirect_pp.pp_value USING utf8) AS redirect_qid,
  rd_namespace AS target_namespace,
  CONVERT(rd_title USING utf8) AS target_title,
  CONVERT(rd_fragment USING utf8) AS target_fragment,
  CONVERT(rd_interwiki USING utf8) AS target_interwiki,
  target_page.page_id AS target_id,
  CONVERT(target_pp.pp_value USING utf8) AS target_qid
FROM
  page AS redirect_page
    JOIN page_props AS redirect_pp ON (redirect_page.page_id=redirect_pp.pp_page AND redirect_pp.pp_propname='wikibase_item')
    LEFT JOIN redirect ON redirect_page.page_id=rd_from
      LEFT JOIN page AS target_page ON (redirect.rd_namespace=target_page.page_namespace AND redirect.rd_title=target_page.page_title)
        LEFT JOIN page_props AS target_pp ON (target_page.page_id=target_pp.pp_page and target_pp.pp_propname='wikibase_item')
WHERE
  redirect_page.page_is_redirect=1"""

    df = query_mediawiki_to_dataframe(database, query)
    if df.shape[0]==0:
        df = pd.DataFrame(
            columns=[
                'redirect_id',
                'redirect_namespace',
                'redirect_title',
                'redirect_qid',
                'target_namespace',
                'target_title',
                'target_fragment',
                'target_interwiki',
                'target_id',
                'target_qid'
            ]
        )
    return df


def query_redirect_badges(url:str) -> pd.DataFrame:
    wd = 'http://www.wikidata.org/entity/'

    query = f"""SELECT ?item ?sitelink ?name ?badge WHERE {{
  VALUES ?badge {{ wd:{QID_S2R} wd:{QID_I2R} }}
  ?sitelink schema:about ?item; schema:isPartOf <{url}/>; schema:name ?name; wikibase:badge ?badge .
}}"""
    columns = {
        'item' : str,
        'sitelink' : str,
        'name' : str,
        'badge' : str
    }

    df = query_wdqs_to_dataframe(query, columns)
    if df.shape[0]==0:
        df = pd.DataFrame(columns=columns.keys())
    df['qid'] = df['item'].str.slice(len(wd))
    df['badge'] = df['badge'].str.slice(len(wd))
    df.drop(
        columns=[
            'item'
        ],
        inplace=True
    )

    return df


def make_master_df(database:Optional[str]=None, url:Optional[str]=None) -> pd.DataFrame:
    if database is None or url is None:
        raise RuntimeWarning('Cannot make master dataframe due to incomplete args')

    redirect_items = query_redirect_pages_linked_to_wikidata_item(database)
    current_badges = query_redirect_badges(url)

    df = redirect_items.merge(
        right=current_badges.loc[current_badges['badge']==QID_S2R],
        how='outer',
        left_on='redirect_qid',
        right_on='qid'
    )
    df.rename(
        columns={
            'qid' : 's2r_qid',
            'sitelink' : 's2r_sitelink',
            'name' : 's2r_name',
            'badge' : 's2r_badge'
        },
        inplace=True
    )

    df = df.merge(
        right=current_badges.loc[current_badges['badge']==QID_I2R],
        how='outer',
        left_on='redirect_qid',
        right_on='qid'
    )
    df.rename(
        columns={
            'qid' : 'i2r_qid',
            'sitelink' : 'i2r_sitelink',
            'name' : 'i2r_name',
            'badge' : 'i2r_badge'
        },
        inplace=True
    )

    return df


def filter_all_redirects(df:pd.DataFrame) -> pd.DataFrame:  # informational only
    filt = df['redirect_id'].notna()
    return df.loc[filt]


def filter_redirects_with_inexistent_target(df:pd.DataFrame) -> pd.DataFrame:  # in order to remove the sitelink
    filt = df['redirect_id'].notna() & df['target_id'].isna()
    return df.loc[filt]


def filter_redirects_with_unconnected_target(df:pd.DataFrame) -> pd.DataFrame:  # TODO: what to do with these?
    filt = df['redirect_id'].notna() & df['target_id'].notna() & df['target_qid'].isna()
    return df.loc[filt]


def filter_redirects_with_any_badge(df:pd.DataFrame) -> pd.DataFrame:  # informational only
    filt = df['redirect_id'].notna() & (df['s2r_badge'].notna() | df['i2r_badge'].notna())
    return df.loc[filt]


def filter_redirects_with_s2r_badge(df:pd.DataFrame) -> pd.DataFrame:  # informational only
    filt = df['redirect_id'].notna() & df['s2r_badge'].notna()
    return df.loc[filt]


def filter_redirects_with_i2r_badge(df:pd.DataFrame) -> pd.DataFrame:  # informational only
    filt = df['redirect_id'].notna() & df['i2r_badge'].notna()
    return df.loc[filt]


def filter_redirects_without_badge(df:pd.DataFrame) -> pd.DataFrame:  # in order to add the s2r badge
    filt = df['redirect_id'].notna() & df['s2r_badge'].isna() & df['i2r_badge'].isna()
    return df.loc[filt]


def filter_redirects_with_both_badges(df:pd.DataFrame) -> pd.DataFrame:  # in order to remove the s2r badge
    filt = df['redirect_id'].notna() & df['s2r_badge'].notna() & df['i2r_badge'].notna()
    return df.loc[filt]


def filter_non_redirects_with_badges(df:pd.DataFrame) -> pd.DataFrame:  # in order to remove s2r and/or i2r badges
    filt = df['redirect_id'].isna()
    return df.loc[filt]


def is_redirect_page(item:pwb.ItemPage, dbname:str) -> bool:
    sitelink = item.sitelinks.get(dbname)

    if sitelink is None:
        touch_pages(item.title(), dbname)
        raise RuntimeWarning(f'No sitelink found for {dbname} in {item.title()}')

    local_page = pwb.Page(source=sitelink)

    return local_page.isRedirectPage()


def target_exists(item:pwb.ItemPage, dbname:str) -> bool:
    sitelink = item.sitelinks.get(dbname)

    if sitelink is None:
        touch_pages(item.title(), dbname)
        raise RuntimeWarning(f'No sitelink found for {dbname} in {item.title()}')

    local_page = pwb.Page(source=sitelink)

    if not local_page.isRedirectPage():
        raise RuntimeWarning(f'Cannot determine target of non-redirect page for {dbname} sitelink in {item.title()}')

    try:
        target_page = local_page.getRedirectTarget()
    except CircularRedirectError as exception:
        raise RuntimeWarning(f'Circular redirect detected for {dbname} sitelink in {item.title()}') from exception
    except InterwikiRedirectPageError as exception:
        raise RuntimeWarning(f'Interwiki redirect detected for {dbname} sitelink in {item.title()}') from exception

    return target_page.exists()


def target_is_connected(item:pwb.ItemPage, dbname:str) -> bool:
    sitelink = item.sitelinks.get(dbname)

    if sitelink is None:
        touch_pages(item.title(), dbname)
        raise RuntimeWarning(f'No sitelink found for {dbname} in {item.title()}')

    local_page = pwb.Page(source=sitelink)

    if not local_page.isRedirectPage():
        raise RuntimeWarning(f'Cannot determine target of non-redirect page for {dbname} sitelink in {item.title()}')

    target_page = local_page.getRedirectTarget()

    if not target_page.exists():
        raise RuntimeWarning(f'Target of redirect page for {dbname} sitelink in {item.title()} does not exist')

    try:
        _ = target_page.data_item()
    except NoPageError:
        return False

    return True


def has_badge(item:pwb.ItemPage, dbname:str, qid_badge:str) -> bool:
    if qid_badge not in [ QID_S2R, QID_I2R ]:
        raise RuntimeWarning(f'Invalid badge {qid_badge} provided for {dbname} in {item.title()}')

    sitelink = item.sitelinks.get(dbname)
    if sitelink is None:
        touch_pages(item.title(), dbname)
        raise RuntimeWarning(f'No sitelink found for {dbname} in {item.title()}')

    if qid_badge in [ badge_item.title() for badge_item in sitelink.badges ]:
        return True

    return False


def get_page_len(item:pwb.ItemPage, dbname:str) -> int:
    sitelink = item.sitelinks.get(dbname)
    if sitelink is None:
        touch_pages(item.title(), dbname)
        raise RuntimeWarning(f'No sitelink found for {dbname} in {item.title()}')

    local_page = pwb.Page(source=sitelink)

    if not local_page.exists():
        raise RuntimeWarning(f'Local page {dbname} sitelink in {item.title()} does not exist')

    return len(local_page.text)


def sitelink_has_any_of_badges(sitelink:pwb.ItemSiteLinkPage, has_none_of_badges:list[str]) -> bool:
    if not sitelink.badges:
        return False

    sitelink_badge_qids = [ badge_item.title() for badge_item in sitelink.badges ]

    for qid_badge in has_none_of_badges:
        if qid_badge in sitelink_badge_qids:
            return True

    return False


def add_badge(item:pwb.ItemPage, dbname:str, qid_badge:str, edit_summary:str, has_none_of_badges:Optional[list[str]]=None) -> None:
    if qid_badge not in [ QID_S2R, QID_I2R ]:
        raise RuntimeError(f'Invalid badge {qid_badge} provided for {dbname} in {item.title()}')

    sitelink = item.sitelinks.get(dbname)
    if sitelink is None:
        touch_pages(item.title(), dbname)
        raise RuntimeWarning(f'No sitelink for {dbname} found in {item.title()}')

    if qid_badge in [ badge_item_page.title() for badge_item_page in sitelink.badges ]:
        raise RuntimeWarning(f'Badge to add {qid_badge} already set for {dbname} sitelink in {item.title()}')

    if has_none_of_badges is not None and sitelink_has_any_of_badges(sitelink, has_none_of_badges):
        raise RuntimeWarning(f'Sitelink does already have an incompatible badge for {dbname} sitelink in {item.title()}; badge to add {qid_badge} omitted')

    new_badges = [
        *sitelink.badges,
        pwb.ItemPage(REPO, qid_badge)
    ]
    new_sitelink = pwb.SiteLink(
        sitelink.canonical_title(),
        site=dbname,
        badges=new_badges
    )

    if SIMULATE is not True:
        try:
            item.setSitelink(
                new_sitelink,
                summary=f'{edit_summary}{EDIT_SUMMARY_APPENDIX}'
            )
        except OtherPageSaveError as exception:
            raise RuntimeWarning(f'Cannot add {dbname} sitelink badge in {item.title()}') from exception

    LOG.info(f'Added badge {qid_badge} to {dbname} sitelink in {item.title()}')


def remove_badge(item:pwb.ItemPage, dbname:str, qid_badge:str, edit_summary:str) -> None:
    if qid_badge not in [ QID_S2R, QID_I2R ]:
        raise RuntimeError(f'Invalid badge {qid_badge} provided for {dbname} in {item.title()}')

    sitelink = item.sitelinks.get(dbname)
    if sitelink is None:
        touch_pages(item.title(), dbname)
        raise RuntimeWarning(f'No sitelink for {dbname} found in {item.title()}')

    new_badges = [ badge_item_page for badge_item_page in sitelink.badges if badge_item_page.title()!=qid_badge ]
    if len(new_badges)==len(sitelink.badges):
        raise RuntimeWarning(f'Badge to remove {qid_badge} not found on {dbname} sitelink in {item.title()}')

    new_sitelink = pwb.SiteLink(
        sitelink.canonical_title(),
        site=dbname,
        badges=new_badges
    )

    if SIMULATE is not True:
        try:
            item.setSitelink(
                new_sitelink,
                summary=f'{edit_summary}{EDIT_SUMMARY_APPENDIX}'
            )
        except OtherPageSaveError as exception:
            raise RuntimeWarning(f'Cannot remove {dbname} sitelink badge in {item.title()}') from exception

    LOG.info(f'Removed badge {qid_badge} from {dbname} sitelink in {item.title()}')


def remove_sitelink(item:pwb.ItemPage, dbname:str, edit_summary:str) -> None:
    if SIMULATE is not True:
        item.removeSitelink(
            dbname,
            summary=f'{edit_summary}{EDIT_SUMMARY_APPENDIX}'
        )

    LOG.info(f'Removed sitelink for {dbname} in {item.title()}')


def touch_pages(qid:str, dbname:str, site:Optional[pwb.Site]=None) -> None:
    if site is None:
        try:
            site = get_site_from_dbname(dbname)
        except RuntimeWarning as exception:
            LOG.warning(f'Cannot instantiate a site object for {dbname} due to exception {exception}; skip job "touch_pages" for project')
            return

    if not site.logged_in():
        LOG.warning(f'Skip touching pages using {qid} in {dbname} (not logged in)')
        return

    params = (qid,)
    query = f"""SELECT
  page_namespace,
  CONVERT(page_title USING utf8) AS page_title
FROM
  page
    JOIN page_props ON page_id=pp_page AND pp_propname='wikibase_item'
WHERE
  pp_value=?"""

    for row in query_mediawiki(dbname, query, params):
        page = pwb.Page(
            source=site,
            title=row.get('page_title'),
            ns=row.get('page_namespace')
        )

        try:
            touch_page(page)
        except RuntimeWarning as exception:
            LOG.warning(exception)
        else:
            LOG.info(f'Touched page {page.title()} on {dbname}')


def touch_page(page:pwb.Page) -> None:
    try:
        page.touch(quiet=True)
    except NoPageError as exception:
        raise RuntimeWarning(f'Cannot touch page {page.title()} on {page.site.sitename} (page does not exist)') from exception
    except APIError as exception:
        raise RuntimeWarning(f'Cannot touch page {page.title()} on {page.site.sitename} (API Error)') from exception
    except CascadeLockedPageError as exception:
        raise RuntimeWarning(f'Cannot touch page {page.title()} on {page.site.sitename} (cascade locked page)') from exception
    except LockedPageError as exception:
        raise RuntimeWarning(f'Cannot touch page {page.title()} on {page.site.sitename} (locked page)') from exception
    except TitleblacklistError as exception:
        raise RuntimeWarning(f'Cannot touch page {page.title()} on {page.site.sitename} (page is blacklisted)') from exception
    except OtherPageSaveError as exception:
        raise RuntimeWarning(f'Cannot touch page {page.title()} on {page.site.sitename} (other reason)') from exception
    except EOFError as exception:
        raise RuntimeWarning(f'Cannot touch page {page.title()} on {page.site.sitename} (terminal input expected, but impossible)') from exception


def get_site_from_dbname(dbname:str) -> pwb.Site:
    try:
        site = pwb.APISite.fromDBName(dbname)
    except UnknownSiteError as exception:
        LOG.warning(exception)
        raise RuntimeWarning(f'Unknown site for dbname {dbname}') from exception
    try:
        site.login(autocreate=True)
    except NoUsernameError as exception:
        LOG.warning(exception)
        raise RuntimeWarning(f'Cannot login to site for dbname {dbname}') from exception

    return site


def process_redirects_with_inexistent_target(df:pd.DataFrame, dbname:Optional[str]=None) -> None:
    if dbname is None:
        raise RuntimeWarning('No valid dbname received to process redirects with inexistent targets')

    filt = df['redirect_id'].notna() & df['target_id'].isna() & df['target_interwiki'].isna()
    for row in df.loc[filt].itertuples():
        item = pwb.ItemPage(REPO, row.redirect_qid)
        try:
            item.get()
        except NoPageError:
            LOG.info(f'Skip {row.redirect_qid} (item page does not exist)')
            continue
        except IsRedirectPageError:
            LOG.info(f'Skip {item.title()} (item page is a redirect)')
            continue

        try:
            check_is_redirect = is_redirect_page(item, dbname)
        except RuntimeWarning as exception:
            LOG.warning(f'Skip {item.title()}, {dbname} sitelink: {exception}')
            continue

        if not check_is_redirect:
            LOG.info(f'Skip {item.title()}, {dbname} sitelink (sitelink to non-redirect, expect redirect)')
            continue

        try:
            check_target_exists = target_exists(item, dbname)
        except RuntimeWarning as exception:
            LOG.warning(f'Skip {item.title()}, {dbname} sitelink: {exception}')
            continue

        if check_target_exists:
            LOG.info(f'Skip {item.title()}, {dbname} sitelink (sitelink target does exist, expect non-exist) ')
            continue

        if get_page_len(item, dbname) > REDIRECT_LENGTH_CUTOFF:
            try:
                add_badge(
                    item,
                    dbname,
                    QID_S2R,
                    f'add badge [[{QID_S2R}]] to {dbname} sitelink; see [[Wikidata:Sitelinks to redirects]] for details',
                    [ QID_I2R ]
                )
            except RuntimeWarning as exception:
                LOG.warning(f'Edit failed in {item.title()}, {dbname} sitelink: {exception}')
            except RuntimeError as exception:
                LOG.error(f'Edit failed in {item.title()}, {dbname} sitelink: {exception}')
                return
        else:
            remove_sitelink(
                item,
                dbname,
                'remove sitelink to redirect page with non-existent target page on client wiki'
            )


def process_redirects_without_badge(df:pd.DataFrame, dbname:Optional[str]=None) -> None:
    if dbname is None:
        raise RuntimeWarning('No valid dbname received to process redirects without bagdes')

    try:
        site = get_site_from_dbname(dbname)
    except RuntimeWarning as exception:
        LOG.warning(f'Cannot instantiate a site object for {dbname} due to exception {exception}; skip job "process_redirects_without_badge" for project')
        return

    filt = df['redirect_id'].notna() & df['target_id'].notna() & df['target_qid'].notna() & df['s2r_badge'].isna() & df['i2r_badge'].isna()
    for row in df.loc[filt].itertuples():
        item = pwb.ItemPage(REPO, row.redirect_qid)
        try:
            item.get()
        except NoPageError:
            LOG.info(f'Skip {row.redirect_qid} (item page does not exist)')
            touch_pages(row.redirect_qid, dbname, site)
            continue
        except IsRedirectPageError:
            LOG.info(f'Skip {item.title()} (item page is a redirect)')
            touch_pages(row.redirect_qid, dbname, site)
            continue

        try:
            check_is_redirect = is_redirect_page(item, dbname)
        except RuntimeWarning as exception:
            LOG.warning(f'Skip {item.title()}, {dbname} sitelink: {exception}')
            continue

        if not check_is_redirect:
            LOG.info(f'Skip {item.title()}, {dbname} sitelink (sitelink to non-redirect, expect redirect)')
            touch_pages(item.title(), dbname, site)
            continue

        try:
            check_target_exists = target_exists(item, dbname)
        except RuntimeWarning as exception:
            LOG.warning(f'Skip {item.title()}, {dbname} sitelink: {exception}')
            continue

        if not check_target_exists:
            LOG.info(f'Skip {item.title()}, {dbname} sitelink (sitelink target does not exist, expect exist) ')
            continue

        if not target_is_connected(item, dbname):
            LOG.info(f'Skip {item.title()}, {dbname} sitelink (sitelink target is not connected, expect connected)')
            continue

        try:
            add_badge(
                item,
                dbname,
                QID_S2R,
                f'add badge [[{QID_S2R}]] to {dbname} sitelink; see [[Wikidata:Sitelinks to redirects]] for details',
                [ QID_I2R ]
            )
        except RuntimeWarning as exception:
            touch_pages(item.title(), dbname, site)
            LOG.warning(f'Edit failed in {item.title()}, {dbname} sitelink: {exception}')
        except RuntimeError as exception:
            LOG.error(f'Edit failed in {item.title()}, {dbname} sitelink: {exception}')
            return


def process_redirects_with_both_badges(df:pd.DataFrame, dbname:Optional[str]=None) -> None:
    if dbname is None:
        raise RuntimeWarning('No valid dbname received to process redirects with both bagdes')

    filt = df['redirect_id'].notna() & df['target_id'].notna() & df['target_qid'].notna() & df['s2r_badge'].notna() & df['i2r_badge'].notna()
    for row in df.loc[filt].itertuples():
        item = pwb.ItemPage(REPO, row.redirect_qid)
        try:
            item.get()
        except NoPageError:
            LOG.info(f'Skip {row.redirect_qid} (item page does not exist)')
            continue
        except IsRedirectPageError:
            LOG.info(f'Skip {item.title()} (item page is a redirect)')
            continue

        try:
            check_is_redirect = is_redirect_page(item, dbname)
        except RuntimeWarning as exception:
            LOG.warning(f'Skip {item.title()}, {dbname} sitelink: {exception}')
            continue

        if not check_is_redirect:
            LOG.info(f'Skip {item.title()}, {dbname} sitelink (sitelink to non-redirect, expect redirect)')
            continue

        try:
            check_target_exists = target_exists(item, dbname)
        except RuntimeWarning as exception:
            LOG.warning(f'Skip {item.title()}, {dbname} sitelink: {exception}')
            continue

        if not check_target_exists:
            LOG.info(f'Skip {item.title()}, {dbname} sitelink (sitelink target does not exist, expect exist) ')
            continue

        if not target_is_connected(item, dbname):
            LOG.info(f'Skip {item.title()}, {dbname} sitelink (sitelink target is not connected, expect connected)')
            continue

        if not has_badge(item, dbname, QID_I2R):
            LOG.info(f'Skip {item.title()}, {dbname} sitelink (sitelink does not have I2R badge, expect has)')
            continue

        try:
            remove_badge(
                item,
                dbname,
                QID_S2R,
                f'remove badge [[{QID_S2R}]] from {dbname} sitelink; [[Wikidata:Sitelinks to redirects|sitelinks to redirect pages]] should not carry both sitelink badges'
            )
        except RuntimeWarning as exception:
            LOG.warning(f'Edit failed in {item.title()}, {dbname} sitelink: {exception}')
        except RuntimeError as exception:
            LOG.error(f'Edit failed in {item.title()}, {dbname} sitelink: {exception}')
            return


def process_non_redirects_with_badges(df:pd.DataFrame, dbname:Optional[str]=None) -> None:
    if dbname is None:
        raise RuntimeWarning('No valid dbname received to process non-redirects with bagdes')

    filt = df['redirect_id'].isna() & df['s2r_badge'].notna()
    for row in df.loc[filt].itertuples():
        item = pwb.ItemPage(REPO, row.s2r_qid)
        try:
            item.get()
        except NoPageError:
            LOG.info(f'Skip {row.s2r_qid} (item page does not exist)')
            continue
        except IsRedirectPageError:
            LOG.info(f'Skip {item.title()} (item page is a redirect)')
            continue

        try:
            check_is_redirect = is_redirect_page(item, dbname)
        except RuntimeWarning as exception:
            LOG.warning(f'Skip {item.title()}, {dbname} sitelink: {exception}')
            continue

        if check_is_redirect:
            LOG.info(f'Skip {item.title()}, {dbname} sitelink (sitelink to redirect, expect non-redirect)')
            continue

        try:
            remove_badge(
                item,
                dbname,
                QID_S2R,
                f'remove badge [[{QID_S2R}]] from {dbname} sitelink; sitelink points to a non-redirect page'
            )
        except RuntimeWarning as exception:
            LOG.warning(f'Edit failed in {item.title()}, {dbname} sitelink: {exception}')
        except RuntimeError as exception:
            LOG.error(f'Edit failed in {item.title()}, {dbname} sitelink: {exception}')
            return

    filt = df['redirect_id'].isna() & df['i2r_badge'].notna()
    for row in df.loc[filt].itertuples():
        item = pwb.ItemPage(REPO, row.i2r_qid)
        try:
            item.get()
        except NoPageError:
            LOG.info(f'Skip {row.i2r_qid} (item page does not exist)')
            continue
        except IsRedirectPageError as exception:
            LOG.info(f'Skip {item.title()} (item page is a redirect)')
            continue

        try:
            check_is_redirect = is_redirect_page(item, dbname)
        except RuntimeWarning as exception:
            LOG.warning(f'Skip {item.title()}, {dbname} sitelink: {exception}')
            continue

        if check_is_redirect:
            LOG.info(f'Skip {item.title()}, {dbname} sitelink (sitelink to redirect, expect non-redirect)')
            continue

        try:
            remove_badge(
                item,
                dbname,
                QID_I2R,
                f'remove badge [[{QID_I2R}]] from {dbname} sitelink; sitelink points to a non-redirect page'
            )
        except RuntimeWarning as exception:
            LOG.warning(f'Edit failed in {item.title()}, {dbname} sitelink: {exception}')
        except RuntimeError as exception:
            LOG.error(f'Edit failed in {item.title()}, {dbname} sitelink: {exception}')
            return


def write_unconnected_redirect_target_report(df:pd.DataFrame, dbname:Optional[str]=None, url:Optional[str]=None, family:Optional[str]=None, language:Optional[str]=None) -> None:
    if dbname is None:
        raise RuntimeWarning('No valid dbname received in write_unconnected_redirect_target_report')
    if url is None:
        raise RuntimeWarning('No valid url received in write_unconnected_redirect_target_report')
    if family is None:
        raise RuntimeWarning('No valid family received in write_unconnected_redirect_target_report')
    if language is None:
        raise RuntimeWarning('No valid language received in write_unconnected_redirect_target_report')

    if df.shape[0] == 0:
        return

    if dbname == 'wikidatawiki':
        redirect_interwiki_prefix = ''
    elif dbname in [ 'commonswiki', 'mediawikiwiki', 'metawiki', 'specieswiki', 'simplewiki' ]:
        redirect_interwiki_prefix = f':{FAMILY_SHORTCUTS.get(dbname, "")}'
    else:
        try:
            redirect_site = pwb.APISite.fromDBName(dbname)
        except UnknownSiteError as exception:
            LOG.warning(exception)
            return  # ignore in report
        redirect_interwiki_prefix = f':{FAMILY_SHORTCUTS.get(redirect_site.family, "w:")}{redirect_site.lang}:'

    namespaces = query_namespaces_from_api(url)

    with open('./output/unconnected_wikitable_body.txt', mode='a', encoding='utf8') as file_handle:
        for elem in df.itertuples():
            if elem.target_interwiki!='':
                target_interwiki_prefix = f'{redirect_interwiki_prefix}{elem.target_interwiki}:'
            else:
                target_interwiki_prefix = redirect_interwiki_prefix

            if int(elem.redirect_namespace)==0:
                redirect_namespace = ''
                redirect_namespace_canonical = ''
            else:
                redirect_namespace = f'{namespaces.get(int(elem.redirect_namespace), {}).get("local", "")}:'
                redirect_namespace_canonical = f' ({namespaces.get(int(elem.redirect_namespace), {}).get("canonical", "")})'

            if int(elem.target_namespace)==0:
                target_namespace = ''
                target_namespace_canonical = ''
            else:
                target_namespace = f'{namespaces.get(int(elem.target_namespace), {}).get("local", "")}:'
                target_namespace_canonical = f' ({namespaces.get(int(elem.target_namespace), {}).get("canonical", "")})'

            file_handle.write('|-\n')
            file_handle.write(f'| {{{{Q|{elem.redirect_qid}}}}}\n')
            file_handle.write(f'| {dbname}\n')
            file_handle.write(f'| [[{redirect_interwiki_prefix}{redirect_namespace}{elem.redirect_title}|{redirect_namespace}{elem.redirect_title.replace("_", " ")}]]{redirect_namespace_canonical}\n')
            file_handle.write(f'| [[{target_interwiki_prefix}{target_namespace}{elem.target_title}|{target_namespace}{elem.target_title.replace("_", " ")}]]{target_namespace_canonical}\n')

    LOG.info(f'Added unconnected target cases to report for {dbname} ({df.shape[0]} entries)')


def finish_unconnected_redirect_target_report() -> None:
    with open('./output/unconnected_wikitable_body.txt', mode='r', encoding='utf8') as file_handle:
        wikitable_body = file_handle.read()

    with open('./output/unconnected_wikitable.txt', mode='w', encoding='utf8') as file_handle:
        file_handle.write('{| class="wikitable"\n')
        file_handle.write('|-\n')
        file_handle.write('! item !! project !! redirect !! unconnected target\n')
        file_handle.write(wikitable_body)
        file_handle.write('|}')

    LOG.info('Finished up report for unconnected redirect target cases')


def write_unconnected_redirect_target_report_to_wiki() -> None:
    with open('./output/unconnected_wikitable.txt', mode='r', encoding='utf8') as file_handle:
        table = file_handle.read()

    report_page = pwb.Page(SITE, REPORT_UNCONNECTED_TARGET)
    report_page.text = f"""This report lists [[Wikidata:Sitelinks to redirects|sitelinks to redirect pages]] on client wikis where the redirect target page is not connected to any Wikidata item. The reported sitelinks to redirects may or may not carry a redirect badge. For most cases, one of these two solutions exist:
* resolve redirect to target, and remove the redirect badge if there is one
* retain the redirect as a sitelink, add the {{{{Q|{QID_I2R}}}}} badge if missing, and connect the target page to another suitable Wikidata item

This report is updated weekly. Last update: {strftime('%e %B %Y')}.

{table}"""
    report_page.save(summary=f'update report{EDIT_SUMMARY_APPENDIX}')

    LOG.info(f'Wrote report for unconnected redirect target cases to page "{REPORT_UNCONNECTED_TARGET}"')


def log_cases_to_tsv_file(df:pd.DataFrame, dbname:Optional[str]=None) -> None:
    if dbname is None:
        raise RuntimeWarning(f'No valid dbname received to log cases')

    with open('./output/cases.tsv', mode='a', encoding='utf8') as file_handle:
        for elem in df.itertuples():
            file_handle.write(f'{dbname}\t' \
                              f'{elem.redirect_qid}\t' \
                              f'{elem.redirect_id}\t' \
                              f'{elem.redirect_namespace}\t' \
                              f'{elem.redirect_title}\t' \
                              f'{elem.target_id}\t' \
                              f'{elem.target_namespace}\t' \
                              f'{elem.target_title}\t' \
                              f'{elem.target_fragment}\t' \
                              f'{elem.target_interwiki}\t' \
                              f'{elem.target_qid}\t' \
                              f'{elem.s2r_sitelink}\t' \
                              f'{elem.s2r_name}\t' \
                              f'{elem.s2r_badge}\t' \
                              f'{elem.i2r_sitelink}\t' \
                              f'{elem.i2r_name}\t' \
                              f'{elem.i2r_badge}\n')


def log_project_stats(payload:dict[str, int], dbname:Optional[str]=None) -> None:
    if dbname is None:
        raise RuntimeWarning(f'No valid dbname received to log project statistics')

    with open('./output/project_stats.tsv', mode='a', encoding='utf8') as file_handle:
        file_handle.write(f'{dbname}\t{payload.get("cnt_all_redirects")}\t' \
                          f'{payload.get("cnt_redirects_with_any_badge")}\t' \
                          f'{payload.get("cnt_redirects_with_s2r_badge")}\t' \
                          f'{payload.get("cnt_redirects_with_i2r_badge")}\t' \
                          f'{payload.get("cnt_redirects_without_badge")}\t' \
                          f'{payload.get("cnt_redirects_with_both_badges")}\t' \
                          f'{payload.get("cnt_non_redirects_with_badges")}\t' \
                          f'{payload.get("cnt_redirects_with_inexistent_target")}\t' \
                          f'{payload.get("cnt_redirects_with_unconnected_target")}\n')


def process_project(project:dict[str, str]) -> None:
    try:
        df = make_master_df(project.get('db_name'), project.get('url'))
    except RuntimeWarning as exception:
        LOG.warning(f'Cannot process project {project} due to exception: {exception}')
        return

    all_redirects = filter_all_redirects(df)
    redirects_with_inexistent_target = filter_redirects_with_inexistent_target(df)  # remove sitelink
    redirects_with_unconnected_target = filter_redirects_with_unconnected_target(df)  # write to report
    redirects_with_any_badge = filter_redirects_with_any_badge(df)
    redirects_with_s2r_badge = filter_redirects_with_s2r_badge(df)
    redirects_with_i2r_badge = filter_redirects_with_i2r_badge(df)
    redirects_without_badge = filter_redirects_without_badge(df)  # add S2R
    redirects_with_both_badges = filter_redirects_with_both_badges(df)  # remove S2R
    non_redirects_with_badges = filter_non_redirects_with_badges(df)  # remove S2R/I2R

    project_stats = {
        'cnt_all_redirects' : all_redirects.shape[0],
        'cnt_redirects_with_any_badge' : redirects_with_any_badge.shape[0],
        'cnt_redirects_with_s2r_badge' : redirects_with_s2r_badge.shape[0],
        'cnt_redirects_with_i2r_badge' : redirects_with_i2r_badge.shape[0],
        'cnt_redirects_without_badge' : redirects_without_badge.shape[0],
        'cnt_redirects_with_both_badges' : redirects_with_both_badges.shape[0],
        'cnt_non_redirects_with_badges' : non_redirects_with_badges.shape[0],
        'cnt_redirects_with_inexistent_target' : redirects_with_inexistent_target.shape[0],
        'cnt_redirects_with_unconnected_target' : redirects_with_unconnected_target.shape[0]
    }

    log_cases_to_tsv_file(all_redirects, project.get('db_name'))
    log_project_stats(payload=project_stats, dbname=project.get('db_name'))
    LOG.info(f'{project.get("db_name"): <20}: {all_redirects.shape[0]:6d} redirects;' \
          f' {redirects_with_any_badge.shape[0]:6d} w/ any badge;' \
          f' {redirects_with_s2r_badge.shape[0]:6d} w/ s2r badge;' \
          f' {redirects_with_i2r_badge.shape[0]:6d} w/ i2r badge;' \
          f' {redirects_without_badge.shape[0]:6d} w/o badge;' \
          f' {redirects_with_both_badges.shape[0]:6d} w/ both badges;' \
          f' {non_redirects_with_badges.shape[0]:6d} non-redirects w/ any badge;' \
          f' {redirects_with_inexistent_target.shape[0]:6d} w/ inexistent target;' \
          f' {redirects_with_unconnected_target.shape[0]:6d} w/ unconnected target')

    if PROCESS_MISSING_S2R_BADGE is True:
        process_redirects_without_badge(
            redirects_without_badge,
            project.get('db_name')
        )

    if PROCESS_BOTH_BADGE_SITUATIONS is True:
        process_redirects_with_both_badges(
            redirects_with_both_badges,
            project.get('db_name')
        )

    if PROCESS_NON_REDIRECTS is True:
        process_non_redirects_with_badges(
            non_redirects_with_badges,
            project.get('db_name')
        )

    if PROCESS_INEXISTENT_TARGETS is True:
        process_redirects_with_inexistent_target(
            redirects_with_inexistent_target,
            project.get('db_name')
        )

    if PROCESS_UNCONNECTED_TARGETS is True:
        write_unconnected_redirect_target_report(
            redirects_with_unconnected_target,
            project.get('db_name'),
            project.get('url'),
            project.get('family'),
            project.get('language')
        )


def main() -> None:
    clear_logfiles()

    projects = query_database_names()
    LOG.info(f'Found {len(projects)} projects with database names')

    for project in projects:
        process_project(project)
        sleep(1)

    if PROCESS_UNCONNECTED_TARGETS is True:
        finish_unconnected_redirect_target_report()
        write_unconnected_redirect_target_report_to_wiki()


if __name__=='__main__':
    main()
