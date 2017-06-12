# -*- coding: utf-8 -*-
"""
Created on Tue Jun  6 15:00:08 2017

@author: Vishal_Oben
"""

import urllib
import sklearn
from sklearn.naive_bayes import GaussianNB
import urllib.request
import json 
import time
from flask import Flask,request
app = Flask(__name__)

@app.route("/test", methods = ['POST'])
def sentiment():
    text = request.json["text"]
    data = urllib.parse.urlencode({"text": text}).encode("utf-8")
    u = urllib.request.urlopen("http://text-processing.com/api/sentiment/", data)
    out= u.read().decode('utf8').replace("'", '"')
    data=json.loads(out)
    s = json.dumps(data, indent=4, sort_keys=True)
    return s

if __name__ == '__main__':
    app.run(
        host = "0.0.0.0",
        port = 80
    )
#print(sentiment("Live is the best place to find the love "))