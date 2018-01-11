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
from src.model.base.database import MySqlDataBase
from src.model.base import config as c

import sys


if __name__ == '__main__':
    db = MySqlDataBase(c.db)

    query = """
    SELECT Code, ifnull(ifnull(ifnull(ifnull(ifnull(L7, L6), L5), L4), L3), L2) AS `description`
    FROM (
        SELECT Code, nullif(L7, '') AS L7, nullif(L6, '') AS L6, nullif(L5, '') AS L5
        , nullif(L4, '') AS L4, nullif(L3, '') AS L3, nullif(L2, '') AS L2
        , nullif(L1, '') AS L1
        FROM agrovoc_autocode.agrovoc_terms
        WHERE `Use?` = 'Y'
    ) as a
    """

    lookup = {}
    db.execute(query)
    for row in db.cursor:
        code = row['Code'].strip()
        description = row['description'].strip().lower()
        lookup[description] = code

    query = """
    SELECT DISTINCT doc_id, text, codes
    FROM agrovoc_autocode.agris_data;
    """

    insert = """
    INSERT INTO agrovoc_autocode.all_data_codes (doc_id, text, code)
    VALUES (%s, %s, %s)
    """

    l_keys = set(lookup.keys())
    size = 0

    db.execute(query)
    data_to_insert = []
    for row in db.cursor:
        doc_id = row['doc_id'].strip()
        text = row['text'].strip()

        code_desc = set([c.strip().lower() for c in row['codes'].split('|')])
        code_desc = l_keys & code_desc
        codes = [lookup[cd] for cd in code_desc]

        if codes:
            for code in codes:
                data_to_insert.append((doc_id, text, code))

            size += len(data_to_insert)
            db_loc = MySqlDataBase(c.db)
            print("Inserted {0} total records".format(size), end='\r')
            sys.stdout.flush()
            db_loc.execute_many(insert, data_to_insert)
            del data_to_insert[:]
            db_loc.teardown()

    db.teardown()
