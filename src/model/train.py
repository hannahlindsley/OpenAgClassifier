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
from src.model.base.model import TextClassifier
from src.model.base.database import MySqlDataBase
from src.model.base import config as c
import re

import numpy as np
import pandas as pd
import scipy.interpolate
from scipy import optimize, signal

import matplotlib.pyplot as plt
import time
import argparse
import sys


def fetch_train_data(columns, hierarchy, limit):
    """
    :param columns: (list) 
    :param hierarchy: (int)
    :param limit: (int)
    :return: (DataFrame)
    """

    db = MySqlDataBase(c.db)

    query = """
    SELECT t1.doc_id, t1.text, group_concat(code SEPARATOR '|') AS targets
    FROM agrovoc_autocode.train_documents t1
    INNER JOIN (
        SELECT DISTINCT {0} 
        FROM agrovoc_autocode.codes_hierarchy
        WHERE {0} IS NOT NULL
    ) b ON t1.code = b.{0}
    WHERE t1.text IS NOT NULL AND t1.text <> ''
    GROUP BY t1.doc_id, text
    ORDER BY rand()
    LIMIT {1};
    """.format(hierarchy, limit)

    db.execute(query)
    data = {col: [] for col in columns}

    for row in db.cursor:
        for col in columns:
            data[col].append(row[col])

    db.teardown()
    return pd.DataFrame(data)


def clean_data(df, columns):
    """
    Removes rows with labels that do not pass certain regular expression checks
    :param df: training data (DataFrame)
    :param columns: column names for training data (list)
    :return: training data with bad rows removed (DataFrame)
    """

    pattern = re.compile("^c_\d+(\|c_\d+)*$")

    filtered_rows = []
    for i, row in df.iterrows():
        col_match = row['%s' % columns]
        col_match = '|'.join([x.strip() for x in col_match.split('|')])

        if re.match(pattern, col_match):
            filtered_rows.append(row)
        else:
            print('\tRemoving row with label "{0}"'.format(col_match), end='\r')
            sys.stdout.flush()

        if (i + 1) % 5000 == 0:
            print('\tChecking rows for errors: {0:.1f}%'.format(100. * i / len(df)), end='\r')
            sys.stdout.flush()
    print('\tChecking rows for errors: 100.0%', end='\r')
    sys.stdout.flush()
    print()
    return pd.DataFrame(filtered_rows)


def get_data(df, indices, columns):
    """
    Parses DataFrame and extracts the text to code, grant labels and grant keys
    :param df: training data (DataFrame)
    :param indices: row numbers to grab (list)
    :param columns: code hierarchy column which contains class labels (list)
    :return: arrays for the text to code, labels and document IDs (tuple of lists)
    """

    documents, labels, doc_ids = [], [], []
    for _, i in enumerate(indices):
        row = df.iloc[i]
        desc = row.text.strip()
        documents.append(desc)

        col_match = row['%s' % columns]
        target = [x.strip() for x in col_match.split('|')]
        labels.append(target)

        doc_ids.append(row.doc_id)
    return documents, labels, doc_ids


def precision_multi(predicted, actual):
    """
    :param predicted: predicted labels (list)
    :param actual: actual labels (list)
    :return: float precision
    """

    return sum([1 for pr, ac in zip(predicted[:, 0].tolist(), actual) if pr in ac]) / float(len(actual))


def check_interval(arr, minimum, maximum):
    """
    Computes the boolean array for a row vector values between a given range.
    :param arr: (ndarray)
    :param minimum: (float)
    :param maximum: (float)
    :return: (ndarray)
    """

    return np.array([minimum <= arr[i] <= maximum for i in range(len(arr))])


def make_hist(decision_matrix):
    """
    This takes the matrix of decision functions, with dimensions of examples x classes,
    and computes the mean of the function results in bins over (-5,5) each 0.1 wide,
    across all of the examples.

    Returns a matrix of <f(x)> (decision function binned values (averaged) for each class)
    , that has dimensions classes x number_of_bins, the bins array, the counts in each bin.
    :param decision_matrix: (ndarray)
    :return: (tuple of ndarrays)
    """

    bins = np.histogram(decision_matrix[0], range=(-5., 5.), bins=100)[1]
    n = np.array([np.histogram(decision_matrix[i, :],
                               range=(-5., 5.),
                               bins=100)[0]
                  for i in range(decision_matrix.shape[0])])

    def mean_of_bin(array):
        if array.shape[0] > 0:
            return np.mean(array)
        return 0

    hist = np.array([[mean_of_bin(decision_matrix[i, check_interval(decision_matrix[i, :], bins[j], bins[j + 1])])
                      for j in range(len(bins) - 1)] for i in range(decision_matrix.shape[0])])
    hist[np.isnan(hist)] = 0
    return hist, bins, n


def get_probabilities(dec_hist, p_a, p_b):
    """
    Constructs the posterior probabilities (sort of) using the method described by John Platt.
    It is only sort of the probability because A does not depend on the classes (which it should,
    but would be computationally very expensive.

    :param dec_hist: numpy matrix array binned decision function values for each class
    :param p_a: Platt parameter A
    :param p_b: Platt parameter B
    :return: Platt probabilities for each binned decision function value, for each class (numpy array)
    """

    prob = 1. / (1. + np.exp(p_a * dec_hist + p_b))
    assert isinstance(prob, np.ndarray)
    prob[prob == 0] = 1e-300
    return prob


def cost_func(prob, y_plus, y_minus):
    """
    Computes the log-loss function, according the original paper from Platt,
    modified to include multi-label/multi-class prediction.  A and B do not
    depend on the class, to make the computations more reasonable.

    :param prob: Platt probabilities (numpy array)
    :param y_plus: positive target computed from Platt's paper for each class and decision function bin (numpy array)
    :param y_minus: negative target computed from Platt's paper for each class and decision function bin (numpy array)
    :return: parameters which minimize cost function, minimized cost function
    """

    prob = prob.T
    assert isinstance(y_plus, np.ndarray)
    assert isinstance(y_minus, np.ndarray)

    return -(1./(np.prod(prob.shape)))*np.trace(np.dot(y_plus, np.log(prob)) + np.dot((1-y_plus), np.log(1-prob)) +
                                                np.dot(y_minus, np.log(prob)) + np.dot((1-y_minus), np.log(1-prob)))


def minimization(decision_hist, y_plus, y_minus):
    """
    This uses the Nelder-Mead optimization algorithm to minimize the log-loss function to learn
    A and B, the Platt parameters.

    This algorithm is not fast, but provides good results.
    :param decision_hist: numpy matrix array binned decision function values for each class
    :param y_plus: positive target computed from Platt's paper for each class and decision function bin (numpy array)
    :param y_minus: negative target computed from Platt's paper for each class and decision function bin (numpy array)
    :return: parameters which minimize cost function, minimized cost function
    """
    def func(x):
        probabilities = np.array(get_probabilities(decision_hist, x[0], x[1]))
        return cost_func(probabilities, y_plus, y_minus)

    res = optimize.minimize(func, (-1, -1), method='Nelder-Mead', options={'disp': True})
    return res.x, func(res.x)


def cross_validate(classifier, train_doc, plot=False):
    """
    Learns the Platt parameters by constructing a log-loss function, and minimizing it
    to find A and B.  Additional documentation for the sub-functions are available in
    their respective definitions.

    :param classifier: binned decision function values for each class (ndarray)
    :param train_doc: text training documents (list)
    :param plot: set to True to find the cost function local minimum graphically (bool, False by default)
    :return: learned Platt parameters (float tuple)
    """

    dec_func_vec = classifier.decision_function(train_doc).T
    decision_hist, bins, num = make_hist(dec_func_vec)
    del dec_func_vec
    end = int(len(bins) / 2)

    # Get all + examples (dec > 0) for each class prediction
    num_pos = np.sum(num[:, end:], axis=1).reshape(num.shape[0], 1)
    # Get all - examples (dec < 0) for each class prediction
    num_minus = np.sum(num[:, :end], axis=1).reshape(num.shape[0], 1)
    n_plus_per_bin = np.hstack((np.zeros((num.shape[0], end)), num_pos * np.ones((num.shape[0], end))))
    n_minus_per_bin = np.hstack((num_minus * np.ones((num.shape[0], end)), np.zeros((num.shape[0], end))))
    del num, num_minus, num_pos

    # y_positive_binned is a matrix where columns are the total number of positive examples for a given class,
    # propagated into each bin, while rows are values per class.
    y_positive_binned = (n_plus_per_bin + 1.) / (n_plus_per_bin + 2.)
    y_negative_binned = 1. / (n_minus_per_bin + 2.)
    del n_plus_per_bin, n_minus_per_bin

    print("[INFO] Minimizing the log-loss function")
    minimized = minimization(decision_hist, y_positive_binned, y_negative_binned)
    p_a, p_b = minimized[0]

    # Plotting to find the minimum of the log-loss function graphically.
    if plot:
        grid = [(dim_a, dim_b) for dim_a in np.arange(-25, -14, 2) for dim_b in np.arange(-25, -14, 2)]
        param, cost = [], []
        for x in grid:
            param.extend([x])
            probabilities = np.array(get_probabilities(decision_hist, x[0], x[1]))
            cost.append(cost_func(probabilities, y_positive_binned, y_negative_binned))
            print(param[-1], cost[-1])

        param_a, param_b = [np.array(z) for z in zip(*param)]
        cost = np.array(cost)
        cost[np.isinf(cost)] = 200
        xi = np.linspace(param_a.min(), param_a.max(), len(param_a))
        yi = np.linspace(param_b.min(), param_b.max(), len(param_b))
        xi, yi = np.meshgrid(xi, yi)

        rbf = scipy.interpolate.Rbf(param_a, param_b, cost, function='linear')
        zi = rbf(xi, yi)

        plt.imshow(zi, aspect='auto', vmin=cost.min(), vmax=cost.max(),
                   extent=[xi.min(), xi.max(), yi.min(), yi.max()], origin='lower')
        plt.colorbar()
        plt.xlabel('Multiplicative coefficient')
        plt.ylabel('Additive coefficient')
        plt.savefig('plots\Cost.png')
        plt.clf()

    return p_a, p_b


def get_stats(predict, plotting=False):
    """
    Takes prior distributions and dynamically finds the part of the distribution that corresponds to the positive
    class prediction
    :param predict: SVM prediction (DataFrame)
    :param plotting: boolean (False by default)
    :return: arrays for positive PDF peak max and initial minimum for the positive PDF for each class
    """

    if plotting:
        # for plotting the score distributions by class
        prob = np.array([list(zip(*x))[1] for x in np.array(predict)])
        labels = np.array([list(zip(*x))[0] for x in np.array(predict)][0])

        for i in range(len(labels)):
            plt.hist(prob[:, i], bins=np.arange(0, 1.01, 0.01))
            plt.savefig('plots\prob_dist_class_%s.png' % (labels[i]))
            plt.clf()

    def find_last_peak(x):
        freq, bins = np.histogram(x)
        max_values = signal.argrelextrema(freq, np.greater)[0]
        new_bins = np.array([(bins[j] + bins[j + 1]) / 2. for j in range(len(bins) - 1)])
        if len(max_values) > 0:
            return new_bins[max_values[-1]]
        else:
            return bins[-1] + np.diff(bins)[0]

    def find_local_min(x):
        freq, bins = np.histogram(x)
        min_values = signal.argrelextrema(freq, np.less)[0]
        new_bins = np.array([(bins[j] + bins[j + 1])/2. for j in range(len(bins) - 1)])
        if len(min_values) > 0:
            return new_bins[min_values[-1]]
        else:
            return bins[-1]

    return predict.apply(find_last_peak), predict.apply(find_local_min)


def learn(percent_val, level, cv=False, save_load=False):
    """
    Main script
    :param percent_val: Percentage of total set to hold out for ad-hoc validation (float)
    :param level: (int) (hierarchy for training)
    :param cv: (bool)
    :param save_load: boolean (False by default, if True then the classifier is saved prematurely and re-loaded for
    testing, this is deprecated as CV / testing is handled separately)
    :return: classifier, training documents, training labels, testing documents, testing labels
    """

    columns = ['doc_id', 'text', 'targets']
    hierarchy = 'hierarchy_{0}'.format(str(level))
    level_col = columns[-1]

    db = MySqlDataBase(c.db)
    query = """
    SELECT COUNT(DISTINCT t1.doc_id) AS c
    FROM agrovoc_autocode.train_documents t1
    INNER JOIN agrovoc_autocode.codes_hierarchy b ON t1.code = b.{0}
    WHERE t1.text IS NOT NULL AND t1.text <> ''
    AND b.{0} IS NOT NULL;""".format(hierarchy)
    db.execute(query)
    count = db.cursor.fetchone()['c']
    print("[INFO] Total number of documents available for training: {0}".format(count))
    db.teardown()

    n_docs = int(count / 5) if cv else count
    n_val = int(percent_val * n_docs)
    n_train = int(n_docs - n_val)

    st = time.time()
    total_df = pd.DataFrame({col: [] for col in columns})
    df = fetch_train_data(columns=columns, hierarchy=hierarchy, limit=n_docs)
    print("[INFO] Number of examples with non-NULL classes: {0}".format(len(df)))
    total_df = total_df.append(df)
    print("\tTime elapsed: ", time.time() - st)

    st = time.time()
    print("[INFO] Ensuring data quality")
    df = clean_data(total_df, columns=level_col)
    print("\tTime elapsed: ", time.time() - st)

    st = time.time()
    print("[INFO] Separating training and validation data...", sep=' ', end='', flush=True)
    np.random.seed(0)
    permuted_indices = np.random.permutation(range(len(df)))
    training_indices = permuted_indices[:n_train]
    val_indices = permuted_indices[n_train:n_train + n_val]

    train_doc, train_labels, doc_ids = get_data(df, training_indices, columns=level_col)
    val_doc, val_labels, doc_ids_val = get_data(df, val_indices, columns=level_col)
    del df, total_df
    print("done.")
    sys.stdout.flush()
    print("\tTime elapsed: ", time.time() - st)

    classifier = TextClassifier()

    st = time.time()
    print("[INFO] Training classifier on {0} examples...".format(str(len(train_doc))), sep=' ', end='', flush=True)
    classifier.train(train_doc, train_labels, doc_ids)
    del doc_ids, doc_ids_val
    print("done.")
    sys.stdout.flush()
    print("\tTime elapsed: ", time.time() - st)

    if save_load:
        st = time.time()
        print("[INFO] Saving classifier...", sep=' ', end='', flush=True)
        filename = classifier.save(path='model/clf_data/', name='cv_save',
                                   platt_a=0., platt_b=0., dist_max=np.array([]), dist_min=np.array([]), in_db=False)
        print("done.")
        sys.stdout.flush()
        print("\tTime elapsed: ", time.time() - st)

        st = time.time()
        print("[INFO] Loading classifier...", sep=' ', end='', flush=True)
        classifier.load(path='model/clf_data/', name=filename, in_db=False)
        print("done.")
        sys.stdout.flush()
        print("\tTime elapsed: ", time.time() - st)

        print("[INFO] Testing classifier...")
        y_val = classifier.predict(val_doc)
        print("\tPrecision:", precision_multi(y_val, val_labels))

    return classifier, train_doc, train_labels, val_doc, val_labels


if __name__ == '__main__':
    level_opt = [1, 2, 3, 4]

    parser = argparse.ArgumentParser()
    parser.add_argument("classifier_name", type=str, help="Human readable name for classifier to be trained")
    parser.add_argument("p_val", type=float, help="Percentage of total set to hold out for ad-hoc validation")
    parser.add_argument("level", type=int, help="Type of classification.  Options: 1, 2, 3, 4")
    parser.add_argument("folds", type=int, help="Number of cross-validation folds to perform.")

    args = parser.parse_args()
    if args.level not in level_opt:
        raise ValueError('Invalid selection.  Level must be {0}.'.format(', '.join(map(str, level_opt))))

    print("[INFO] Performing a {0}-fold cross-validation to learn the Platt parameters".format(str(args.folds)))
    platt_a, platt_b = 0, 0
    for _ in range(args.folds):
        print()
        clf, training_docs, training_labels, _, _ = learn(percent_val=args.p_val, level=args.level, cv=True)
        a, b = cross_validate(clf, training_docs)
        del clf, training_docs, training_labels

        platt_a += a
        platt_b += b

    platt_a /= args.folds
    platt_b /= args.folds
    print("\n[INFO] Best fit Platt parameters (A: %s, B: %s)" % (platt_a, platt_b))

    print("\n[INFO] Training on full set using the Platt learned parameters")
    clf, training_docs, training_labels, _, _ = learn(percent_val=args.p_val, level=args.level, cv=False)
    predictions = pd.DataFrame(clf.grid_predict(training_docs, platt_a, platt_b, low_memory=True))

    dist_max, dist_min = get_stats(predictions, plotting=False)
    del predictions

    start = time.time()
    print("[INFO] Saving classifier...", end='\r')
    sys.stdout.flush()
    file_name = clf.save(path='model/clf_data/',
                         name=args.classifier_name,
                         platt_a=platt_a,
                         platt_b=platt_b,
                         dist_max=dist_max.as_matrix(),
                         dist_min=dist_min.as_matrix(),
                         in_db=False)
    print("saved with root name {0}.".format(file_name))
    print("\tTime elapsed: ", time.time() - start)
