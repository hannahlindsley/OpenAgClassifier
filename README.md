# AGROVOC autocoder

This Open-Ag auto-classification model is a product of 
[Foundation Center](http://foundationcenter.org/). It was developed
by Dave Hollander (dfh@foundationcenter.org) and Bereketab Lakew 
(bkl@foundationcenter.org). This project uses Python 3.5.x in order to handle all
UTF-8 encoding issues. 

Training data for this model was obtained from 
[Food and Agriculture Organization of the United Nations](http://agris.fao.org/agris-search/index.do).

The predictions are available as a free service through the Foundation Center's API via the 
`/text/ag_classification` REST API endpoint at [api.foundationcenter.org](https://api.foundationcenter.org).

If you wish to host the model locally, the pre-trained models can be 
[downloaded](https://s3.amazonaws.com/fc-public/svm/open_ag_models.zip), and 
should be stored in `src\model\clf_data\`. They can be served using the Flask
server included in this project.

## Install

This project contains the scraping code used for extracting training data
from FAO AGROVOC. The first two commands are only necessary if you don't have python, numpy, scipy, and sklearn. 
If you would like to be able to run the scraping script
then please run the following commands from the command line (note: this is not
supported in Windows or Ubuntu):

    #brew install python3
    #pip3 install -U numpy scipy scikit-learn
    pip3 install -r requirements.txt
    python3 db/create_db.py
    python3 src/get_data.py
    
Note that getting the data takes a very long time, to avoid overloading the website.

## Train the model

After setting up the MySQL environment, and training data has been collected using
the included scraping scripts, the models can be trained for each hierarchy by running:

    python train.py model_name validation_percent hierarchy_level CV_folds
    
from within `src/model`. An example of training the model for the first
AGROVOC code hierarchy is

    python train.py model_h1 0.1 1 3
    
where the model will be named model_h1, we hold 10% of the input set for 
on-the-fly cross-validation, and we are doing 3-fold cross-validation. Models
are pickled in `src/data` with a hashed unique-identifier
as a name prefix to prevent model overwrites.

## Predict using the model

The prediction API is served up using Flask which supports both GET
and POST methods. The models are loaded into memory, which requires >=20GB
of RAM, when the server is started at the beginning of time. The base name
of each classifier should be specified for loading the models, ensuring
that the correct models are used for each classifier (ex. clf1 should load
the hierarchy 1 model). Thresholds in the config file for the models have
been determined through cross-validation.

A local MySQL environment should be setup according to
[the MySQL Setup section](#user-content-mysql-setup); specifically you will need
the codes_hierarchy table to read the AGROVOC taxonomy set into
a lookup dictionary.

To start the API server simply run

    python model/server.py
    
from within `src/`.

The API accepts the following parameters:

    text (string) : This is the text will be passed through the models to predict the relevant AGROVOC terms. This can be anything from
    a single word up to multi-page documents with many paragraphs.
    
    chunk (string) : Input text can be broken up into individual sentences that are predicted separately, and the results are 
    aggregated. This parameter toggles this feature on and off (accpets 'true' and 'false', optional, 'false' by default).
    
    threshold (string) : (optional, set to 'high' to only accept predictions with a high confidence score).
    
    rollup (string) : Some AGROVOC codes are 'children' of more generic taxonomy terms. The model by default tries to return
    the most specific codes, only. Setting this parameter to 'false' will also return 'parent' codes.
    ex) The text 'apples' will predict the 'apples' and 'fruit' AGROVOC codes, but 'fruit' is the parent of 'apples', and so
    with rollup turned on only 'apples' will be returned. (accpets 'true' and 'false', optional, 'true' by default).
    
    form (string) : specify whether the response is returned in XML (JSON by default, accepts 'xml')
    
To make a call to the API from Python you can do:

```python
import json
from urllib import request

url = "http://hostname:9091/predict"
opts = {"text": "I want to grow apples. I'm interested in raising cows.",
        "chunk": "true",
        "threshold": "low",
        "rollup": "false"
        }
        
req = request.Request(url, data=json.dumps(opts).encode('utf8'), headers={"Content-Type": "application/json"})
response = request.urlopen(req).read().decode('utf8')
print(response)
```

For processing multiple documents, there is a batch prediction 
API endpoint which can process each document asynchronously. In 
order to submit jobs to this endpoint the passed data should be
in the following JSON structure:

```json
{
    "doc_key_1": {
        "text": "text_1"
    },
    "doc_key_2": {
        "text": "text_2"
    },
    "doc_key_3": {
        "text": "text_3"
    },
    "doc_key_4": {
        "text": "text_4"
    }
}
```

The batch endpoint only accepts POST requests, and returns predictions
as a JSON with the document key values as the unique identifiers
for each set of predictions. Queries can be made such as

```python
import json
from urllib import request

url = "http://hostname:9091/batch"
data = {"doc_key_1": {"text": "text_1"},
        "doc_key_2": {"text": "text_2"},
        "doc_key_3": {"text": "text_3"},
        "doc_key_4": {"text": "text_4"}}

opts = {"data": data,
        "chunk": "true",
        "threshold": "low",
        "rollup": "true"
        }

req = request.Request(url, data=json.dumps(opts).encode('utf8'), headers={"Content-Type": "application/json"})
response = request.urlopen(req).read().decode('utf8')
print(response)
```
