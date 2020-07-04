# Code for patching pkoc: adds 334 reports to the corpus

import pickle
import csv
from nltk.tokenize import word_tokenize
import urllib3
import re
import string
from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import os
import csv
import requests
from bs4 import BeautifulSoup
import pickle
from datetime import datetime
import shutil
import glob
import stanfordnlp
import pickle
import nltk
from nltk.stem import PorterStemmer
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.tokenize import RegexpTokenizer

################
# 1. Functions #
################

def rearrange_keys(old_keys = [], months_list = [], countries_path = ''):

    missions_info = missions_master_path
    missions_master = []
    with open(missions_info) as inputfile:
        for line in inputfile:
            missions_master.append(line.strip().split(','))

    new_keys = []

    for key in old_keys:
        #
        # take the mission
        #
        mission = key.split('p')[0]
        mission = mission.replace('c', '')

        print(mission)

        if mission == 'ONUCI':
            mission = 'UNOCI'

        if mission == 'UNFOR':
            mission = 'UNPROFOR'

        #
        # take the code
        #
        code = key.split('n')[-1]
        #
        # generate file name
        #
        name = code.replace('/', '_')

        #
        # take the month
        #
        for m in months:
            if m in key:
                month = m
        #
        # take the year
        #
        year = code.split('/')[1]
        #
        # take the period
        #
        period = key.split('p')[1]
        period = period.split('m')[0]
        #
        # take mission country
        #
        try:
            mission_country = [e for a, b, c, d, e, f in missions_master if a == mission][0]
        except:
            mission_country = 'none'

        new_key = '-'.join([mission,code,name,period,year,month,mission_country])
        print(new_key)
        new_keys.append([key, new_key])

    return new_keys
def corpus_create_rpkoc(plain_corpus = {}):

    rpkoc = {}
    stop_words = set(stopwords.words('english'))

    for a in plain_corpus:
        print(a)

        if type(plain_corpus[a]) == str:
            rpkoc[a] = plain_corpus[a].translate(str.maketrans('', '', string.punctuation)) # remove punctuation from each document
            rpkoc[a] = word_tokenize(rpkoc[a])
        else:
            rpkoc[a] = plain_corpus[a]

        rpkoc[a] = [w for w in rpkoc[a] if not w in stop_words]
        rpkoc[a] = ' '.join(rpkoc[a]).split()
        porter = PorterStemmer()
        rpkoc[a] = [porter.stem(w) for w in rpkoc[a]]
        rpkoc[a] = [w for w in rpkoc[a] if len(w)>1]
        rpkoc[a] = [w for w in rpkoc[a] if not any(c.isdigit() for c in w)]

    return rpkoc
def corpus_create_tpkoc(plain_corpus = {}):

    tpkoc = {}

    for i in range(0,len(plain_corpus)):
        print(i+1,'/',len(plain_corpus))

        if type(plain_corpus[list(plain_corpus)[i]]) == str:
            text = word_tokenize(plain_corpus[list(plain_corpus)[i]])
        else:
            text = plain_corpus[list(plain_corpus)[i]]
            text = [t for t in text if t != '']

        tpkoc[list(plain_corpus)[i]] = nltk.pos_tag(text)

    return tpkoc

#######################
# 2. Data preparation #
#######################

# import ppkoc
#
corpus_dictionaries_path = "/home/ea/Documents/working_on/PKOC_2019/Archive/corpus/corpus_dictionaries_archive/"
ppkoc = pickle.load(open(corpus_dictionaries_path+"ppkoc.p", "rb"))

# import old ppkoc
#
old_corpus_dictionaries_path = "/home/ea/Documents/working_on/PKOC_2019/utils/old_versions/"
old_ppkoc = pickle.load(open(old_corpus_dictionaries_path+"pPKOC.p", "rb"), encoding='latin1')

# reports codes from old pkoc
#
old_ppkoc_keys = list(old_ppkoc.keys())
old_ppkoc_codes = [x.split('n')[-1] for x in old_ppkoc_keys]

# reports codes from new pkoc
#
ppkoc_keys = list(ppkoc.keys())
ppkoc_codes = [x.split('-')[1] for x in ppkoc_keys]

# codes in old pkoc not in new pkoc
#
difference_codes = [x for x in old_ppkoc_codes if x not in ppkoc_codes]
len(difference_codes) # 334

# Take the reports from old pkoc corresponding to the difference
#
missing_dict = {}
for k,v in old_ppkoc.items():
    for code in difference_codes:
        if code == k.split('n')[-1]:
            w = ' '.join(v)
            missing_dict[k] = w.replace('  ', ' ')
len(missing_dict) # 334

# Now we need to format the keys from old pkoc same as the keys from new pkoc..
# take the keys
#
old_keys_missing = []
for k in missing_dict.keys():
    old_keys_missing.append(k)
# old format: 'cUNOMIGp89mJanuaryy2001nS/2001/59']
# new format: 'UNAMI-S/2018/359-S_2018_359-177-2018-April-IRQ'
# the order is different  and in old is missing the report name and the country

# rearranging the order of old keys
#
months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
missions_master_path = "/home/ea/Documents/working_on/PKOC_2019/utils/periods/missions_master.txt"

reference_keys = rearrange_keys(old_keys = old_keys_missing, months_list = months, countries_path = missions_master_path)

###############
# 3. Patching #
###############

## Patching ppkoc
#
ppkoc_patch = {}

for k,v in missing_dict.items():
    for r in reference_keys:
        print(r)
        if k == r[0]:
            ppkoc_patch[r[1]] = v

patched_ppkoc = ppkoc
patched_ppkoc.update(ppkoc_patch)

## Patching rpkoc
#
patched_rpkoc = corpus_create_rpkoc(plain_corpus = patched_ppkoc)

## Patching tpkoc
#
patched_tpkoc = corpus_create_tpkoc(plain_corpus = patched_ppkoc)

###########
# 4. Save #
###########

corpus_patched_dictionaries_path = "/home/ea/Documents/working_on/PKOC_2019/Archive/corpus/corpus_patched_dictionaries_archive/"
pickle.dump(patched_ppkoc, open(corpus_patched_dictionaries_path+"ppkoc.p", "wb"))
pickle.dump(patched_rpkoc, open(corpus_patched_dictionaries_path+"rpkoc.p", "wb"))
pickle.dump(patched_tpkoc, open(corpus_patched_dictionaries_path+"tpkoc.p", "wb"))