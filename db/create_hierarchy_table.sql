-- Copyright 2017 Foundation Center. All Rights Reserved.
--
-- Licensed under the Foundation Center Public License, Version 1.1 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://gis.foundationcenter.org/licenses/LICENSE-1.1.html
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
-- ==============================================================================

CREATE TABLE IF NOT EXISTS`agrovoc_autocode`.`codes_hierarchy` (
  `hierarchy_1` varchar(10) DEFAULT NULL,
  `hierarchy_2` varchar(10) DEFAULT NULL,
  `hierarchy_3` varchar(10) DEFAULT NULL,
  `hierarchy_4` varchar(10) DEFAULT NULL,
  `hierarchy_5` varchar(10) DEFAULT NULL,
  KEY `h4_index` (`hierarchy_4`),
  KEY `h5_index` (`hierarchy_5`),
  KEY `h3_index` (`hierarchy_3`),
  KEY `h2_index` (`hierarchy_2`),
  KEY `h1_index` (`hierarchy_1`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

TRUNCATE TABLE agrovoc_autocode.codes_hierarchy;

SELECT Code as hierarchy_5
, (SELECT Code FROM agrovoc_autocode.agrovoc_terms t WHERE t.L6 = a.L6 AND t.L7 = '' AND t.`Use?` = 'Y') as hierarchy_4
, (SELECT Code FROM agrovoc_autocode.agrovoc_terms t WHERE t.L5 = a.L5 AND t.L6 = '' AND t.L7 = '' AND t.`Use?` = 'Y') as hierarchy_3
, (SELECT Code FROM agrovoc_autocode.agrovoc_terms t WHERE t.L4 = a.L4 AND t.L5 = '' AND t.L6 = '' AND t.L7 = '' AND t.`Use?` = 'Y') as hierarchy_2
, (SELECT Code FROM agrovoc_autocode.agrovoc_terms t WHERE t.L3 = a.L3 AND t.L4 = '' AND t.L5 = '' AND t.L6 = '' AND t.L7 = '' AND t.`Use?` = 'Y') as hierarchy_1
FROM agrovoc_autocode.agrovoc_terms a
WHERE `Use?` = 'Y'
AND L7 <> '';


INSERT INTO agrovoc_autocode.codes_hierarchy (hierarchy_4, hierarchy_3, hierarchy_2, hierarchy_1)
SELECT Code as hierarchy_4
, (SELECT Code FROM agrovoc_autocode.agrovoc_terms t WHERE t.L5 = a.L5 AND t.L6 = '' AND t.L7 = '' AND t.`Use?` = 'Y') as hierarchy_3
, (SELECT Code FROM agrovoc_autocode.agrovoc_terms t WHERE t.L4 = a.L4 AND t.L5 = '' AND t.L6 = '' AND t.L7 = '' AND t.`Use?` = 'Y') as hierarchy_2
, (SELECT Code FROM agrovoc_autocode.agrovoc_terms t WHERE t.L3 = a.L3 AND t.L4 = '' AND t.L5 = '' AND t.L6 = '' AND t.L7 = '' AND t.`Use?` = 'Y') as hierarchy_1
FROM agrovoc_autocode.agrovoc_terms a
WHERE `Use?` = 'Y'
AND L6 <> ''
AND L7 = '';


INSERT INTO agrovoc_autocode.codes_hierarchy (hierarchy_3, hierarchy_2, hierarchy_1)
SELECT Code as hierarchy_3
, (SELECT Code FROM agrovoc_autocode.agrovoc_terms t WHERE t.L4 = a.L4 AND t.L5 = '' AND t.L6 = '' AND t.L7 = '' AND t.`Use?` = 'Y') as hierarchy_2
, (SELECT Code FROM agrovoc_autocode.agrovoc_terms t WHERE t.L3 = a.L3 AND t.L4 = '' AND t.L5 = '' AND t.L6 = '' AND t.L7 = '' AND t.`Use?` = 'Y') as hierarchy_1
FROM agrovoc_autocode.agrovoc_terms a
WHERE `Use?` = 'Y'
AND L5 <> ''
AND L6 = '' AND L7 = '';


INSERT INTO agrovoc_autocode.codes_hierarchy (hierarchy_2, hierarchy_1)
SELECT Code as hierarchy_2
, (SELECT Code FROM agrovoc_autocode.agrovoc_terms t WHERE t.L3 = a.L3 AND t.L4 = '' AND t.L5 = '' AND t.L6 = '' AND t.L7 = '' AND t.`Use?` = 'Y') as hierarchy_1
FROM agrovoc_autocode.agrovoc_terms a
WHERE `Use?` = 'Y'
AND L4 <> ''
AND L5 = '' AND L6 = '' AND L7 = '';


INSERT INTO agrovoc_autocode.codes_hierarchy (hierarchy_1)
SELECT Code as hierarchy_1
FROM agrovoc_autocode.agrovoc_terms a
WHERE `Use?` = 'Y'
AND L3 <> ''
AND L4 = '' AND L5 = '' AND L6 = '' AND L7 = '';


CREATE TABLE IF NOT EXISTS `all_data_codes` (
  `doc_id` varchar(50) NOT NULL,
  `text` varchar(4000) DEFAULT NULL,
  `code` varchar(10) DEFAULT NULL,
  KEY `code_index` (`code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;