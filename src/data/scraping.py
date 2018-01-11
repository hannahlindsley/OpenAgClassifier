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
from selenium import webdriver
from urllib.parse import urlencode
from urllib.request import urlretrieve
from bs4 import BeautifulSoup
import re


class FAOScraper:

    def __init__(self, url=None):
        self.search_prefix = "http://agris.fao.org/agris-search/"
        self.base = url if url is not None else "/searchIndex.action?".format(self.search_prefix)
        self.xml_prefix = "{}/export!endNote.do?".format(self.search_prefix)
        self.start_index_search = 0
        self.num_pages = None
        self.current_page = 1
        self.type_string = ''
        self.sort_field = 'Relevance'
        self.sort_order = 'Descending'
        self.enable_field = 'Disable'
        self.params = ['query',
                       'startIndexSearch',
                       'agrovocString',
                       'typeString',
                       'sortField',
                       'sortOrder',
                       'enableField']
        self.session = webdriver.Chrome()

    def _read(self, url=None):
        if url is None:
            url = self.base
        self.session.get(url)
        soup = BeautifulSoup(self.session.page_source, "html.parser")
        return soup

    @staticmethod
    def _download(url, filename):
        urlretrieve(url, filename)

    def _attributes(self, q='*', ag_str=''):
        args = [q,
                self.start_index_search,
                ag_str, self.type_string,
                self.sort_field,
                self.sort_order,
                self.enable_field]

        params = list(zip(self.params, args))
        return self.base + urlencode(params)

    def _get_num_pages(self):
        item = self.session.find_element_by_css_selector(".pull-xs-left.pag-gotolink").text
        if item:
            n_pages = item.split(' ')[2]
        else:
            n_pages = 0
        self.num_pages = int(n_pages)
        if self.num_pages == 0:
            self.num_pages = 1

    def _paginate(self):
        self.start_index_search += 10
        self.current_page += 1

    def get_search_results(self, ag_str, q='*'):
        """
        Runs searches on the AGRIS engine, handles pagination and sends results links to other functions

        :param ag_str: AGROVOC term (str)
        :param q: query (str)
        :return: result URLs (list)
        """
        url = self._attributes(q=q, ag_str=ag_str)
        page = self._read(url)
        if self.num_pages is None:
            self._get_num_pages()
        results = page.select(".search-results .result-item .inner .inner h3 a")
        self._paginate()
        return [item['href'] for item in results if item is not None]

    def get_item(self, address):
        """
        Scrapes the title, abstract and AGROVOC codes from the given address

        :param address: (str)
        :return: title, abstract, terms (list) (tuple)
        """
        url = self.search_prefix + address
        page = self._read(url)
        title = page.select_one("article div h1")
        title = title.string.strip() if title is not None else ""
        title = re.sub(r"\s+", " ", title)
        abstract = page.select("article .abstract .row")
        abstract = [re.sub(r"\s+", " ", item.string.strip()) for item in abstract if item.string is not None]
        abstract = abstract[0] if abstract else ""
        terms = page.select(".mashup-blocks div .agrovoc_keywords div ul")
        terms = [item['href'].split('/')[-1] for block in terms for item in block.select("li a")]
        return title, abstract, terms

    def get_xml(self, address, file_location):
        """
        Downloads the XML for the particular record in the address

        :param address: (str)
        :param file_location: (str)
        """
        item_id = address.split("=")[-1]
        address = "arn=" + item_id
        url = self.xml_prefix + address
        self._download(url, file_location + item_id + '.xml')
        return item_id
