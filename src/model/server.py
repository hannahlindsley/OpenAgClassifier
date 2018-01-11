# -*- coding: utf-8 -*-
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
from src.model.base.prediction import Predictor
from src.model.base.model import TextClassifier
from nltk.data import load

from src.model.base.database import MySqlDataBase
from src.model.base.graph import run, bfs
from src.model.base import config as c
from src.model.json_to_xml import dicttoxml

import json
import os
import time
import warnings
import itertools
from concurrent.futures import ThreadPoolExecutor, as_completed

from flask import Flask, request, Response
from flask_cors import cross_origin


app = Flask(__name__)
warnings.simplefilter("ignore", UserWarning)


def _load_to_memory(name, level):
    clf = TextClassifier()
    clf.load(path='model/clf_data/', name=name, in_db=False)
    del clf.doc_ids
    return Predictor(classifier=clf, high_t=c.high_threshold[level], low_t=c.low_threshold[level])


def _get_lookup():
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
    db.execute(query)

    d = {}
    for row in db.cursor:
        code = row["Code"].strip()
        description = row["description"].strip()

        d[code] = description
    db.teardown()
    return d


def _validate(js, k):
    return isinstance(js, dict) and k in js


def taxonomy_rollup(results):
    """
    Does the taxonomy rollup using a graph breadth-first-search
    algorithm
    :param results: (list of dictionaries)
    :return: (list of dictionaries)
    """

    all_codes = set([r["code"] for r in results])
    to_keep = set()
    node_check = all_codes - to_keep

    for n in node_check:
        to_keep.add(n)
        k = bfs(graph=graph, start=n, to_check=node_check, keep=to_keep)
        to_keep.add(k)

    return [r for r in results if r["code"] in to_keep if r["code"] is not None]


def predict(text, chunk, threshold, rollup, idx=None):
    """
    Main prediction function
    :param text: (str)
    :param chunk: (bool)
    :param threshold: use high or low cut on the confidence scores (str, accepts 'high' or 'low')
    :param rollup: do code hierarchy (graph) rollup to most specific (bool)
    :param idx: (str, optional)
    :return: (dict)
    """

    if not text:
        agg = [{"code": None, "description": None, "confidence": 0.0}]
        if idx is None:
            return agg
        return {idx: agg}

    # text, chunk, threshold, rollup, idx = obj
    if chunk:
        text = [sub for sent in sentence_detector.tokenize(text) for sub in sent.split(';')]

    # get all predictions, for every hierarchy asynchronously
    results = []
    with ThreadPoolExecutor(max_workers=4) as executor:
        future_results = {executor.submit(func, (text, lookup, threshold)): idx + 1
                          for idx, func in enumerate([p1.predict,
                                                      p2.predict,
                                                      p3.predict,
                                                      p4.predict
                                                      ])}
        for future in as_completed(future_results):
            results.extend(future.result())

    # resolve duplication that arises due to chunking (accept the result with the maximum confidence per class)
    if chunk:
        results_sort = sorted(results, key=lambda x: (x["code"], x["confidence"]))
        grouped = itertools.groupby(results_sort, lambda s: s["code"])
        results = [max(v, key=lambda x: x["confidence"]) for k, v in grouped]

    # add logic to toggle the agrovoc graph roll up on and off
    if rollup:
        agg = taxonomy_rollup(results)
    else:
        agg = [r for r in results if r["code"] is not None]

    if not agg:
        agg = [{"code": None, "description": None, "confidence": 0.0}]

    agg = sorted(agg, key=lambda s: s["confidence"], reverse=True)

    if idx is None:
        return agg
    return {idx: agg}


def array_predict(obj):
    """
    Batched prediction sub-routine
    :param obj: [text, chunk, threshold, rollup, ID] (list)
    :return: (list of dicts)
    """

    all_predictions = []
    for text, chunk, threshold, rollup, idx in obj:
        r = predict(text, chunk, threshold, rollup, idx)
        all_predictions.append(r)

    return all_predictions


print("[INFO] Loading AGROVOC classifiers")
p1 = _load_to_memory(name='hierarchy_1_76021167-b4ce-463d-bab0-bc7fb044b74b', level=1)
p2 = _load_to_memory(name='hierarchy_2_2fd8b6a0-6786-42ef-9eea-66ea02a1dfdd', level=2)
p3 = _load_to_memory(name='hierarchy_3_2b946288-5eeb-4d35-a1fe-6987c118c3b5', level=3)
p4 = _load_to_memory(name='hierarchy_4_3e787d47-5183-4df2-ba4b-509926f029d3', level=4)

lookup = _get_lookup()
graph = run(MySqlDataBase(c.db))
sentence_detector = load("tokenizers/punkt/english.pickle")


@app.route('/predict', methods=['POST', 'GET'])
@cross_origin(origin='*', headers=['Content-Type', 'Authorization'])
def single():
    """
    Single text predictions
    :return: (either JSON or XML based on passed 'form' parameter, JSON by default)
    """

    j = request.get_json()
    if j is None:
        j = request.args
    if not j:
        j = request.form

    threshold = 0
    rollup = True
    chunk = False
    xml = False

    if 'chunk' in j and j['chunk'].lower() == 'true':
        chunk = True
    if 'threshold' in j and j['threshold'] == 'high':
        threshold = 1
    if 'form' in j and j['form'] == 'xml':
        xml = True
    if 'roll_up' in j and j['roll_up'].lower() == 'false':
        rollup = False

    if _validate(j, 'text'):
        st = time.time()

        text = j['text']
        agg = predict(text=text, chunk=chunk, threshold=threshold, rollup=rollup, idx=None)
        response = {"success": True, "duration": time.time() - st, "data": agg}

        if xml:
            return Response(response=dicttoxml(response), status=200, mimetype='text/xml')
        return Response(response=json.dumps(response, indent=4), status=200, mimetype='application/json')

    response = {"success": False, "status": "Incorrect parameters"}
    if xml:
        return Response(response=dicttoxml(response), status=200, mimetype='text/xml')
    return Response(response=json.dumps(response, indent=4), status=404, mimetype='application/json')


@app.route('/batch', methods=['POST'])
@cross_origin(origin='*', headers=['Content-Type', 'Authorization'])
def batch():
    """
   Batch, asynchronous text predictions. Only accepts POST requests
   :return: (either JSON or XML based on passed 'form' parameter, JSON by default)
   """

    j = request.get_json()
    if not j:
        j = request.form

    threshold = 0
    rollup = True
    chunk = False
    xml = False

    if 'chunk' in j and j['chunk'].lower() == 'true':
        chunk = True
    if 'threshold' in j and j['threshold'] == 'high':
        threshold = 1
    if 'form' in j and j['form'] == 'xml':
        xml = True
    if 'roll_up' in j and j['roll_up'].lower() == 'false':
        rollup = False

    if _validate(j, 'data') and isinstance(j['data'], dict):
        st = time.time()
        text_batch = []
        for k in j['data']:
            text = j['data'][k]['text'] if 'text' in j['data'][k] else ''
            text_batch.append([text, chunk, threshold, rollup, k])

        master = [text_batch[int((i / 4) * len(text_batch)): int(((i + 1) / 4) * len(text_batch))] for i in range(4)]
        results = []
        with ThreadPoolExecutor(max_workers=4) as executor:
            future_results = {executor.submit(array_predict, batch_item): idx + 1
                              for idx, batch_item in enumerate(master)}
            for future in as_completed(future_results):
                results.append(future.result())

        response = {"success": True, "duration": time.time() - st, "data": results}

        if xml:
            return Response(response=dicttoxml(response), status=200, mimetype='text/xml')
        return Response(response=json.dumps(response, indent=4), status=200, mimetype='application/json')

    response = {"success": False, "status": "Incorrect parameters"}
    if xml:
        return Response(response=dicttoxml(response), status=200, mimetype='text/xml')
    return Response(response=json.dumps(response, indent=4), status=404, mimetype='application/json')


if __name__ == '__main__':
    debug = os.environ.get('DEBUG', False)
    port = os.environ.get('PORT', 9091)
    testing = os.environ.get('TESTING', False)
    app.run(host='0.0.0.0', port=port, debug=debug)
