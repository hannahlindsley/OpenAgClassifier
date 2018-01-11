"""
# Copyright 2017 Foundation Center. All Rights Reserved.
#
# Licensed under the Foundation Center Public License, Version 1.1 (the “License”);
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://gis.foundationcenter.org/licenses/LICENSE-1.1.html
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an “AS IS” BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==============================================================================
"""
from src.data.scraping import FAOScraper
from sqlalchemy import create_engine
from src.model.base import config as c
from xml.etree import ElementTree as Et
import os
from time import sleep
import pandas as pd


host = c.db["SERVER"]
user = c.db["UID"]
port = c.db["PORT"]
pw = c.db["PWD"]
db = create_engine("mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8".format(user, pw, host, port, "agrovoc"))


def get_doc_ids():
    """
    Gets all doc_ids in the db if re-running the scraping.

    :return: AGRIS document IDs (set)
    """
    query = "SELECT DISTINCT doc_id, search_term FROM agrovoc_autocode.agris_data"
    doc_ids = pd.read_sql_query(query, con=db)
    doc_ids['doc_id'].apply(lambda x: x.strip())
    doc_ids['search_term'].apply(lambda x: x.strip())
    return doc_ids


def get_codes():
    """
    Gets all AGROVOC terms of interest

    :return: AGROVOC codes set, AGROVOC descriptions set (tuple)
    """
    query = """
            SELECT Code, 
                   ifnull(ifnull(ifnull(ifnull(ifnull(L7, L6), L5), L4), L3), L2) 
            AS `description`
            FROM (
                SELECT Code, nullif(L7, '') AS L7, nullif(L6, '') AS L6, nullif(L5, '') AS L5
                , nullif(L4, '') AS L4, nullif(L3, '') AS L3, nullif(L2, '') AS L2
                , nullif(L1, '') AS L1
                FROM agrovoc_autocode.agrovoc_terms
                WHERE `Use?` = 'Y'
            ) as a
            """
    df = pd.read_sql_query(query, con=db)
    df['Code'].apply(lambda x: x.strip())
    df['description'].apply(lambda x: x.strip())
    return df


def scrape(scraper, agrovoc_term, all_codes):
    """
    Runs AGRIS search, scrapes all results from the JavaScript

    :param scraper: scraping class (class)
    :param agrovoc_term: (str)
    :param all_codes: (set)
    :return: results list, page number, total number of pages (tuple)
    """
    page = scraper.get_search_results(ag_str=agrovoc_term)
    page_num, total_pages = s.current_page, s.num_pages

    results = []
    for _, link in enumerate(page):
        title, abstract, codes = scraper.get_item(link)
        codes = set(codes)
        codes = codes & all_codes  # intersection

        if len(codes) > 0:
            text_to_store = title + " - " + abstract
            results.append((link, text_to_store[:4000], ";".join(list(codes)), str(page_num - 1), agrovoc_term))

    return results, page_num, total_pages


def scrape_from_xml(scraper, agrovoc_term, ids=None):
    """
    Runs search on AGRIS, downloads XMLs for every returned record and kicks off processing

    :param scraper: scraping class (class)
    :param agrovoc_term: (str)
    :param ids: document IDs already captured (set)
    :return: results list, page number, total number of pages (tuple)
    """
    page = scraper.get_search_results(ag_str=agrovoc_term)
    page_num, total_pages = scraper.current_page, scraper.num_pages
    results = []

    for link in page:

        if link.split("=")[-1] in ids:
            continue

        item_id = scraper.get_xml(link, 'data/xml/')
        meta_data = process_xml('data/xml/' + item_id + '.xml', page_num, agrovoc_term)
        meta_data = list(meta_data)

        results.append(meta_data[0])

    return results


def process_xml(filename, page_num, agrovoc_term):
    """
    Extracts the title, abstract and AGROVOC codes from temporary XML files stored locally.

    :param filename: (str)
    :param page_num: (int)
    :param agrovoc_term: (str)
    :return: doc_id, text, codes, page number, AGROVOC term (tuple)
    """
    tree = Et.parse(filename)
    root = tree.getroot()

    doc_id = filename.split('/')[-1].split('.')[0]

    for block in root.findall('records'):
        for record in block.findall('record'):
            title = " - ".join([elem.text for elem in record.find('titles')])
            abstracts = [elem.text for elem in record.findall('abstract')]
            abstract = " - ".join(abstracts) if abstracts else ''
            codes = "|".join([elem.text.lower() for elem in record.find('keywords')])
            text_to_store = title + " - " + abstract
            yield (doc_id, text_to_store[:4000], codes, str(page_num - 1), agrovoc_term)
    os.remove(filename)


def collect_docs():
    # pn is page number, tp is total number of pages
    items_to_insert = scrape_from_xml(s, ag_desc, ids=docs['doc_id'].values)
    if items_to_insert:
        print("Inserting {0} records".format(len(items_to_insert)))
        db.execute(insert, items_to_insert)
        sleep(2)


if __name__ == '__main__':
    codes = get_codes()
    docs = get_doc_ids()    # docs is an empty df the first time the scraper is run (it's where results are stored)

    insert = """
             INSERT INTO agrovoc_autocode.agris_data (doc_id, text, codes, page, search_term)
             VALUES (%s, %s, %s, %s, %s)
             """

    for idx, ag_desc in enumerate(codes['description'].drop_duplicates().values):
        s = FAOScraper()
        if ag_desc in docs['search_term'].values:
            continue
        iterate = True
        while iterate:
            print("[INFO] Processing search term {0}".format(ag_desc))
            try:
                collect_docs()
            except IndexError:
                print("[INFO] IndexError on search term {0}, moving on...".format(ag_desc))
                iterate = False
                s.session.close()
                continue
            except Exception as ex:
                print("[INFO] An error occurred: {0}".format(ex))
                s.current_page += 1
                s.start_index_search += 10
            if s.current_page - 1 >= s.num_pages:
                iterate = False
                s.session.close()
