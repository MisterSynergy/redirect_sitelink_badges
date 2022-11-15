from collections.abc import Generator
from io import StringIO
import logging
import logging.config
from os.path import expanduser
from time import sleep
from typing import Any, Optional

import pandas as pd
import pywikibot as pwb
from pywikibot.exceptions import OtherPageSaveError
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

REDIRECT_LENGTH_CUTOFF = 100  # bytes; longer redirect pages are treated as valid even if the target does not exist
EDIT_SUMMARY_APPENDIX:str = ' #msynbotTask10'

PROCESS_MISSING_S2R_BADGE = False
PROCESS_BOTH_BADGE_SITUATIONS = False
PROCESS_NON_REDIRECTS = False
PROCESS_INEXISTENT_TARGETS = False
PROCESS_UNCONNECTED_TARGETS = False
SIMULATE = True


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


def query_mediawiki(database:str, query:str) -> Generator[dict, None, None]:
    with Replica(database) as db_cursor:
        db_cursor.execute(query)
        result = db_cursor.fetchall()

        for row in result:
            for key, value in row.items():  # binary fields need to be converted to string
                if isinstance(value, bytes) or isinstance(value, bytearray):
                    row[key] = value.decode('utf8')
            yield row


def query_mediawiki_to_dataframe(database:str, query:str) -> pd.DataFrame:
    with Replica(database) as db_cursor:
        db_cursor.execute(query)
        result = db_cursor.fetchall()

    df = pd.DataFrame(data=result)

    for column in df.columns:
        if not pd.api.types.is_string_dtype(df[column]):
            continue
        df[column] = df[column].str.decode('utf8')

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


def redirect_pages_linked_to_wikidata_item(database:str='enwiki') -> pd.DataFrame:    
    query = """SELECT
  redirect_page.page_id AS redirect_id,
  redirect_page.page_namespace AS redirect_namespace,
  redirect_page.page_title AS redirect_title,
  redirect_pp.pp_value AS redirect_qid,
  rd_namespace AS target_namespace,
  rd_title AS target_title,
  rd_fragment AS target_fragment,
  rd_interwiki AS target_interwiki,
  target_page.page_id AS target_id,
  target_pp.pp_value AS target_qid
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

    redirect_items = redirect_pages_linked_to_wikidata_item(database)
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


def query_database_names() -> list[dict[str, str]]:
    # as in https://quarry.wmcloud.org/query/12744
    query = """SELECT dbname, url FROM wiki WHERE is_closed=0 AND has_wikidata=1"""
    db_names = []
    for row in query_mediawiki('meta', query):
        db_names.append({'db_name' : row['dbname'], 'url' : row['url']})

    return db_names


def clear_logfiles() -> None:
    for logfile in [ './output/cases.tsv', './output/project_stats.tsv', './output/unconnected_wikitable_body.txt' ]:
        open(logfile, mode='w', encoding='utf8').write('')


def is_redirect_page(item:pwb.ItemPage, dbname:str) -> bool:
    sitelink = item.sitelinks.get(dbname)
    
    if sitelink is None:
        raise RuntimeWarning(f'No sitelink found for {dbname} in {item.title()}')
    
    local_page = pwb.Page(source=sitelink)
    
    return local_page.isRedirectPage()


def target_exists(item:pwb.ItemPage, dbname:str) -> bool:
    sitelink = item.sitelinks.get(dbname)

    if sitelink is None:
        raise RuntimeWarning(f'No sitelink found for {dbname} in {item.title()}')
    
    local_page = pwb.Page(source=sitelink)

    if not local_page.isRedirectPage():
        raise RuntimeWarning(f'Cannot determine target of non-redirect page for {dbname} sitelink in {item.title()}')

    target_page = local_page.getRedirectTarget()

    return target_page.exists()


def target_is_connected(item:pwb.ItemPage, dbname:str) -> bool:
    sitelink = item.sitelinks.get(dbname)

    if sitelink is None:
        raise RuntimeWarning(f'No sitelink found for {dbname} in {item.title()}')
    
    local_page = pwb.Page(source=sitelink)

    if not local_page.isRedirectPage():
        raise RuntimeWarning(f'Cannot determine target of non-redirect page for {dbname} sitelink in {item.title()}')

    target_page = local_page.getRedirectTarget()

    if not target_page.exists():
        raise RuntimeWarning(f'Target of redirect page for {dbname} sitelink in {item.title()} does not exist')
    
    try:
        _ = target_page.data_item()
    except pwb.exceptions.NoPageError as exception:
        return False

    return True


def has_badge(item:pwb.ItemPage, dbname:str, qid_badge:str) -> bool:
    if qid_badge not in [ QID_S2R, QID_I2R ]:
        raise RuntimeWarning(f'Invalid badge {qid_badge} provided for {dbname} in {item.title()}')

    sitelink = item.sitelinks.get(dbname)
    if sitelink is None:
        raise RuntimeWarning(f'No sitelink found for {dbname} in {item.title()}')

    if qid_badge in [ badge_item.title() for badge_item in sitelink.badges ]:
        return True

    return False


def get_page_len(item:pwb.ItemPage, dbname:str) -> int:
    sitelink = item.sitelinks.get(dbname)
    if sitelink is None:
        raise RuntimeWarning(f'No sitelink found for {dbname} in {item.title()}')

    local_page = pwb.Page(source=sitelink)

    if not local_page.exists():
        raise RuntimeWarning(f'Local page {dbname} sitelink in {item.title()} does not exist')

    return len(local_page.text)


def add_badge(item:pwb.ItemPage, dbname:str, qid_badge:str, edit_summary:str) -> None:
    if qid_badge not in [ QID_S2R, QID_I2R ]:
        raise RuntimeWarning(f'Invalid badge {qid_badge} provided for {dbname} in {item.title()}')

    sitelink = item.sitelinks.get(dbname)
    if sitelink is None:
        raise RuntimeWarning(f'No sitelink for {dbname} found in {item.title()}')

    if qid_badge in [ badge_item_page.title() for badge_item_page in sitelink.badges ]:
        raise RuntimeWarning(f'Badge to add {qid_badge} already set for {dbname} sitelink in {item.title()}')

    new_badges = [
        *sitelink.badges,
        pwb.ItemPage(REPO, qid_badge)
    ]
    new_sitelink = pwb.SiteLink(
        sitelink.title,
        site=dbname,
        badges=new_badges
    )

    if SIMULATE is not True:
        item.setSitelink(
            new_sitelink,
            summary=f'{edit_summary}{EDIT_SUMMARY_APPENDIX}'
        )

    LOG.info(f'Added badge {qid_badge} to {dbname} sitelink in {item.title()}')


def remove_badge(item:pwb.ItemPage, dbname:str, qid_badge:str, edit_summary:str) -> None:
    if qid_badge not in [ QID_S2R, QID_I2R ]:
        raise RuntimeWarning(f'Invalid badge {qid_badge} provided for {dbname} in {item.title()}')

    sitelink = item.sitelinks.get(dbname)
    if sitelink is None:
        raise RuntimeWarning(f'No sitelink for {dbname} found in {item.title()}')

    new_badges = [ badge_item_page for badge_item_page in sitelink.badges if badge_item_page.title()!=qid_badge ]
    if len(new_badges)==len(sitelink.badges):
        raise RuntimeWarning(f'Badge to remove {qid_badge} not found on {dbname} sitelink in {item.title()}')

    new_sitelink = pwb.SiteLink(
        sitelink.title,
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


def process_redirects_with_inexistent_target(df:pd.DataFrame, dbname:Optional[str]=None) -> None:
    if dbname is None:
        return
    
    filt = df['redirect_id'].notna() & df['target_id'].isna() & df['target_interwiki'].isna()
    for row in df.loc[filt].itertuples():
        item = pwb.ItemPage(REPO, row.redirect_qid)
        item.get()

        if not is_redirect_page(item, dbname):
            continue

        if target_exists(item, dbname):
            continue

        if get_page_len(item, dbname) > REDIRECT_LENGTH_CUTOFF:
            try:
                add_badge(
                    item,
                    dbname,
                    QID_S2R,
                    f'add badge [[{QID_S2R}]] to {dbname} sitelink; see [[Wikidata:Sitelinks to redirects]] for details'
                )
            except RuntimeWarning:
                pass
        else:
            remove_sitelink(
                item,
                dbname,
                'remove sitelink to redirect page with non-existent target page on client wiki'
            )


def process_redirects_without_badge(df:pd.DataFrame, dbname:Optional[str]=None) -> None:
    if dbname is None:
        return

    filt = df['redirect_id'].notna() & df['target_id'].notna() & df['target_qid'].notna() & df['s2r_badge'].isna() & df['i2r_badge'].isna()
    for row in df.loc[filt].itertuples():
        item = pwb.ItemPage(REPO, row.redirect_qid)
        item.get()

        if not is_redirect_page(item, dbname):
            continue

        if not target_exists(item, dbname):
            continue

        if not target_is_connected(item, dbname):
            continue

        try:
            add_badge(
                item,
                dbname,
                QID_S2R,
                f'add badge [[{QID_S2R}]] to {dbname} sitelink; see [[Wikidata:Sitelinks to redirects]] for details'
            )
        except RuntimeWarning:
            pass

        
def process_redirects_with_both_badges(df:pd.DataFrame, dbname:Optional[str]=None) -> None:
    if dbname is None:
        return
    
    filt = df['redirect_id'].notna() & df['target_id'].notna() & df['target_qid'].notna() & df['s2r_badge'].notna() & df['i2r_badge'].notna()
    for row in df.loc[filt].itertuples():
        item = pwb.ItemPage(REPO, row.redirect_qid)
        item.get()

        if not is_redirect_page(item, dbname):
            continue

        if not target_exists(item, dbname):
            continue

        if not target_is_connected(item, dbname):
            continue

        if not has_badge(item, dbname, QID_I2R):
            continue

        try:
            remove_badge(
                item,
                dbname,
                QID_S2R,
                f'remove badge [[{QID_S2R}]] from {dbname} sitelink; [[Wikidata:Sitelinks to redirects|sitelinks to redirect pages]] should not carry both sitelink badges'
            )
        except RuntimeWarning:
            pass
        

def process_non_redirects_with_badges(df:pd.DataFrame, dbname:Optional[str]=None) -> None:
    if dbname is None:
        return

    filt = df['redirect_id'].isna() & df['s2r_badge'].notna()
    for row in df.loc[filt].itertuples():
        item = pwb.ItemPage(REPO, row.s2r_qid)
        item.get()

        if is_redirect_page(item, dbname):
            continue

        try:
            remove_badge(
                item,
                dbname,
                QID_S2R,
                f'remove badge [[{QID_S2R}]] from {dbname} sitelink; sitelink points to a non-redirect page'
            )
        except RuntimeWarning:
            pass


    filt = df['redirect_id'].isna() & df['i2r_badge'].notna()
    for row in df.loc[filt].itertuples():
        item = pwb.ItemPage(REPO, row.i2r_qid)
        item.get()

        if is_redirect_page(item, dbname):
            continue

        try:
            remove_badge(
                item,
                dbname,
                QID_I2R,
                f'remove badge [[{QID_I2R}]] from {dbname} sitelink; sitelink points to a non-redirect page'
            )
        except RuntimeWarning:
            pass


def write_unconnected_redirect_target_report(df:pd.DataFrame, dbname:Optional[str]=None) -> None:
    if dbname is None:
        raise RuntimeWarning('No valid dbname received in write_unconnected_redirect_target_report')

    with open('./output/unconnected_wikitable_body.txt', mode='a', encoding='utf8') as file_handle:
        for elem in df.itertuples():
            file_handle.write('|-\n')
            file_handle.write(f'| {{{{Q|{elem.redirect_qid}}}}} || {dbname} || {elem.redirect_title} || {elem.target_title}\n')

    LOG.debug(f'Wrote unconnected target report for {dbname} with {df.shape[0]} entries')


def finish_unconnected_redirect_target_report() -> None:
    with open('./output/unconnected_wikitable_body.txt', mode='r', encoding='utf8') as file_handle:
        wikitable_body = file_handle.read()

    with open('./output/unconnected_wikitable.txt', mode='w', encoding='utf8') as file_handle:
        file_handle.write('{| class="wikitable"\n')
        file_handle.write('|-\n')
        file_handle.write('! item !! project !! redirect !! unconnected target\n')
        file_handle.write(wikitable_body)
        file_handle.write('|}')


def log_cases_to_tsv_file(df:pd.DataFrame, dbname:Optional[str]=None) -> None:
    if dbname is None:
        return

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
        return

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

    log_cases_to_tsv_file(all_redirects, project.get('dbname'))
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
        try:
            write_unconnected_redirect_target_report(
                redirects_with_unconnected_target,
                project.get('db_name')
            )
        except NotImplementedError:
            pass


def main() -> None:
    clear_logfiles()

    projects = query_database_names()
    LOG.info(f'Found {len(projects)} projects with database names')

    for project in projects:
        process_project(project)
        sleep(1)

    finish_unconnected_redirect_target_report()


if __name__=='__main__':
    main()
