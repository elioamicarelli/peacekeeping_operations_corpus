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

path = os.getcwd()
os.chdir(path)

driver_path = "PKOC/utils/chromedriver"
missions_master_path = "PKOC/utils/periods/missions_master.txt"
missions_periods_path = "PKOC/utils/periods/missions_periods.txt"

index_path = 'PKOC/Archive/index/index_archive/'
index_title = "index_2019-07-17.txt"
reports_path = "PKOC/Archive/reports/reports_archive/"
reports_path_doc = "PKOC/Archive/reports/reports_word_archive/"
texts_path = "PKOC/Archive/reports/texts_archive/"
isolated_reports_path = "PKOC/Archive/reports/isolated_reports_archive/"
isolated_texts_path = "PKOC/Archive/reports/isolated_texts_archive/"
irrelevant_texts_path = "PKOC/Archive/reports/irrelevant_texts_archive/"
corpus_texts_path = "PKOC/Archive/corpus/corpus_texts_archive/"
corpus_dictionaries_path = "PKOC/Archive/corpus/corpus_dictionaries_archive/"
metadata_path = "PKOC/Archive/corpus/metadata/"
filtered_texts_index_path = "PKOC/index_filtered_texts/"

index_title = ""
links_index = []
texts_removed = []
exc = []
mykeys = []
ppkoc = {}

### Utils functions ###

def copy_files(files = exc, destination = ''):
    for file in files:
        filename = file.split('/')[-1]
        shutil.copy(file, destination + filename)

### Archive functions ###

## 1 Index functions

## 1.1 build_index() returns a list of links to relevant reports for all years. If 'save' = True the index is saved in 'index_archive'.

def build_index(save = False, index_archive = index_path):

    # 1 Obtain link to years pages

    r = requests.get('https://www.un.org/securitycouncil/content/reports-secretary-general')
    c = r.content
    soup = BeautifulSoup(c)

    years_link = []
    for a in soup.findAll('a', href=True):
        if 'submitted' in a['href']:
            years_link.append((a['href']))

    # 2 Obtain link to reports for each year

    index = []

    for i in years_link:
        print('https://www.un.org/'+ i)
        r = requests.get('https://www.un.org/'+ i)
        c = r.content
        soup = BeautifulSoup(c)
        main_content = soup.find('div', attrs = {'class': 'field-items'})
        for link in main_content.findAll('a', attrs={'href': re.compile("^http")}):
            index.append(link.get('href'))

    if save == True:
        save_file = index_archive + 'index_' + datetime.now().strftime('%Y-%m-%d') + '.txt'
        with open(save_file, "wb") as fp:  # Pickling
            pickle.dump(index, fp)

    return(index)

## 1.2 import_index() import an index specified from a path with a name

def import_index(index_archive = index_path, index_name = index_title):

    with open(index_archive + index_name, "rb") as ind:
    index = pickle.load(ind)

    return(index)

## 1.3 update_index() check if an index needs to be updated. If so and 'save' = True it saves the new index and the addendum (addendum is the difference between the old index and the new index)

def update_index(save = False, index_archive = index_path, index_name = index_title):

    index_1 = import_index(index_archive = index_path, index_name = index_title)

    index_2 = build_index(save = False, index_archive = index_path)

    diff_index = list(set(index_2 - index_1))

    if len(diff_index) == 0:
        print("No update needed")

    if len(diff_index) > 0:

        print("There are " + len(diff_index) + " new reports")

    if save == True:

        save_index = index_archive + 'index_' + datetime.now().strftime('%Y-%m-%d') + '.txt'

        with open(save_index, "wb") as fp:
            pickle.dump(index_2, fp)

        save_addendum = index_archive + 'addendum_' + datetime.now().strftime('%Y-%m-%d') + '.txt'

        with open(save_addendum, "wb") as fp:
        pickle.dump(diff_index, fp)

        print("Index saved as " + save_index)
        print("Addendum saves as " + save_addendum)

## 2 Reports functions

# 2.1 download_reports() downloads reports as specified in an index into a reports folder

def download_reports(index = links_index, driver = driver_path, doc = False, reports_archive = reports_path):

    chrome_profile = webdriver.ChromeOptions()
    profile = {"plugins.plugins_list": [{"enabled": False, "name": "Chrome PDF Viewer"}],
               "download.default_directory": reports_archive}
    chrome_profile.add_experimental_option("prefs", profile)
    browser = webdriver.Chrome(executable_path = driver, options = chrome_profile)

    if doc:
        for link in links_index:
            r = requests.get(link)
            c = r.content
            soup = BeautifulSoup(c)

            #download_url = [a['href'] for a in soup.findAll('a', href=True) if 'DOC' in a['href']]
            for a in soup.findAll('a', href=True):
               if 'DOC' in a['href']:
                   download_url = 'https://daccess'+a['href'].split('daccess')[1]
                   print(download_url)
                   browser.get(download_url)
                   time.sleep(3)
    else:
        for i in index:
            print(i)
            download_url = re.sub('org.*S', 'org/pdf?symbol=en/S',i)
            browser.get(download_url)
            time.sleep(3)

# 2.2 match_index_reports() compares the reports downloaded in the archive with the index in order to find how many are missing

def match_index_reports(index = links_index, reports_archive = reports_path):

    codes = [re.search(r'[A-Z]+.+$', i).group(0) for i in index]
    files = [f for r, d, f in os.walk(reports_path)][0]
    files_formatted = [i.replace('_', '/') for i in files]
    files_formatted = [i[:-6] for i in files_formatted]
    codes_files_diff = [i for i in codes if i not in files_formatted]

    print("There are " + str(len(codes)) + " codes in index")
    print("There are " + str(len(files_formatted)) + " codes in files")
    print("The difference is " + str(len(codes) - len(files_formatted)))
    print(codes_files_diff)
    return codes_files_diff

# 2.3 isolate_wrong_conversions_reports() copies or moves reports that were not correctly converted to a separate archive

def isolate_reports(reports_archive = reports_path,
                    texts_archive = texts_path,
                    isolated_reports_archive = isolated_reports_path,
                    min_length = 5, clean_destination = False, move = False, copy = False):

    if clean_destination:
        to_clean = glob.glob(isolated_reports_archive+"/*")
        for i in to_clean:
            os.remove(i)

    files = [f for r, d, f in os.walk(texts_archive)][0]
    length_dic = {}

    for file in files:
        with open(texts_archive + file) as infile:
            words = 0
            characters = 0
            for lineno, line in enumerate(infile, 1):
                wordslist = line.split()
                words += len(wordslist)
                characters += sum(len(word) for word in wordslist)
            length_dic.update({file: [lineno, words, characters]})

    short_texts = [k for k, v in length_dic.items() if float(v[0]) <= float(min_length)]
    print("Number of files isolated: " + str(len(short_texts)))

    short_reports = [i[:-4] for i in short_texts]

    if move:
        for report in short_reports:
            shutil.move(reports_archive + report, isolated_reports_archive + report)

    if copy:
        for report in short_reports:
            shutil.copy(reports_archive + report, isolated_reports_archive + report)

    return short_reports

# 2.4 isolate_wrong_conversions_texts()

def isolate_texts(texts_archive = texts_path,
                  isolated_texts_archive = isolated_texts_path,
                  min_length = 5, clean_destination = False, move = False, copy = False):

    if clean_destination:
        to_clean = glob.glob(isolated_texts_archive+"/*")
        for i in to_clean:
            os.remove(i)

    files = [f for r, d, f in os.walk(texts_archive)][0]
    length_dic = {}

    for file in files:
        with open(texts_archive + file) as infile:
            words = 0
            characters = 0
            for lineno, line in enumerate(infile, 1):
                wordslist = line.split()
                words += len(wordslist)
                characters += sum(len(word) for word in wordslist)
            length_dic.update({file: [lineno, words, characters]})

    short_texts = [k for k, v in length_dic.items() if float(v[0]) <= float(min_length)]
    print("Number of files isolated: " + str(len(short_texts)))

    if move:
        for text in short_texts:
            shutil.move(texts_archive + text, isolated_texts_archive + text)

    if copy:
        for text in short_texts:
            shutil.copy(texts_archive + text, isolated_texts_archive + text)

    return short_texts

# 2.5 isolate_non_missions_texts() first takes the difference between text archive and isolated_text_archive and then check which files contains the name of a mission in the first part

def filter_non_missions_texts(texts_archive = texts_path,
                              isolated_texts_archive = isolated_texts_path,
                              irrelevant_texts_archive = irrelevant_texts_path,
                              missions_info = missions_master_path,
                              clean_destination = False, move = False, copy = False):

    if clean_destination:
        to_clean = glob.glob(irrelevant_texts_archive+"/*")
        for i in to_clean:
            os.remove(i)

    missions_master = []
    with open(missions_info) as inputfile:
        for line in inputfile:
            missions_master.append(line.strip().split(','))

    missions = [a for a, b, c, d, e, f in missions_master]

    text_path = [os.path.split(f) for f in glob.glob(texts_archive + "*.txt")]
    texts = [title for path, title in text_path]

    isolated_text_path = [os.path.split(f) for f in glob.glob(isolated_texts_archive + "*.txt")]
    isolated_texts = [title for path, title in isolated_text_path]

    filter_texts = [t for t in texts if t not in isolated_texts]

    positives = []
    negatives = []

    for i in range(0, len(filter_texts)):
        try:
            print(str(i) + "/" + str(len(filter_texts)))
            # 2.1 import text of file 1
            f = open(texts_archive+filter_texts[i], 'r')
            report = f.read().replace('\n', ' ')
            # 2.2 get mission name
            intro = report.split("II.")[0]
            capital = filter(None, [x.strip() for x in re.findall(r"\b[A-Z\s]*\b", intro)])
            capital = [b for b in capital if len(b) > 2]
            capital = [x for x in capital if x != '']
            mission_name = [x for x in capital if x in missions][0]

            print("mission_name:" + mission_name)
            positives.append((filter_texts[i], mission_name))
        except Exception as e:  # most generic exception you can catch
            print('ops')
            negatives.append((filter_texts[i], str(e)))

    negatives_path = [texts_path + a for a,b in negatives]

    if move:
        for text,warning in negatives:
            shutil.move(texts_archive + text, irrelevant_texts_archive + text)

    if copy:
        for text,warning in negatives:
            shutil.copy(texts_archive + text, irrelevant_texts_archive + text)

    return positives, negatives


# 2.6 reports_metadata() generates metadata for each report and return a dictionary

def reports_metadata(texts_archive = texts_path,
                     metadata_archive = metadata_path,
                     missions_info = missions_master_path,
                     missions_periods = missions_periods_path):

    files = glob.glob(texts_archive + "*.txt")
    logfile = open(metadata_archive + "metadata_errorlog", "w")

    missions_master = []
    with open(missions_info) as inputfile:
        for line in inputfile:
            missions_master.append(line.strip().split(','))

    missions_list = [a for a,b,c,d,e,f in missions_master]

    missions_periods_keys = []
    with open(missions_periods) as inputfile:
        for line in inputfile:
            missions_periods_keys.append(line.strip().split(','))

    keys = [[a, c, d] for a, b, c, d, e, f in missions_periods_keys]

    missions_keys = []

    exceptions = []

    for i in range(0, len(files)):
        try:

            print(str(i+1) + "/" + str(len(files)) + ": " + files[i].split("/")[-1])

            # 2.1 import text
            f = open(files[i], 'r')
            report = f.read().replace('\n', ' ')

            # 2.2 get mission name
            #intro = report.split("II.")[0]
            #capital = re.sub('[^A-Z ]', '', intro)
            #capital = capital.split("  ")
            #capital = [x.strip() for x in capital]
            #capital = [x for x in capital if x != '']
            #capital = [x for x in capital if len(x) > 2]
            #mission_name = [x for x in capital if x in missions_list][0]
            #print('mission name: '+ mission_name)

            intro = report.split("II.")[0]
            capital = filter(None, [x.strip() for x in re.findall(r"\b[A-Z\s]*\b", intro)])
            capital = [b for b in capital if len(b) > 2]
            capital = [x for x in capital if x != '']
            mission_name = [x for x in capital if x in missions_list][0]

            if mission_name == 'ONUCI':
                mission_name = 'UNOCI'

            if mission_name == 'UNFOR':
               mission_name = 'UNPROFOR'

            print('mission name: ' + mission_name)

            # 2.3 extract code report
            pattern = re.compile('S/\d+/\d+')
            report_code = pattern.search(report[0:100]).group()
            print('report code: '+ report_code)

            # 2.4 extract year and month
            patternyear = re.compile('S/(\d+)/')
            report_year = re.findall(patternyear, report_code)[0]
            print('year: '+str(report_year))

            patternmonth = re.compile('January|February|March|April|May|June|July|August|September|October|November|December')
            report_month = patternmonth.search(report[0:100]).group()
            print('month: '+ report_month)

            # 2.5 Extract period
            mission_period_key = mission_name + str(report_year) + report_month
            print(mission_period_key)
            mission_period = [b for a, b, c in keys if c == mission_period_key][0]
            print('period: '+ str(mission_period))

            # 2.6 get mission country
            mission_country = [e for a, b, c, d, e, f in missions_master if a == mission_name][0]
            print('mission country: ' + mission_country)
            print(mission_name + report_code + str(mission_period) + str(report_year) + report_month + mission_country)

            # 2.7 mission text title
            title = files[i].split('/')[-1]
            title = title[:-10]

            metadata = mission_name + '-' +  report_code + '-' + title + '-' + str(mission_period) + '-' + str(report_year) + '-' + report_month + '-' + mission_country
            missions_keys.append((report_code, mission_name, metadata, files[i]))

        except Exception as e:  # most generic exception you can catch
            logfile.write(str(i) + " " + files[i] + ": " + str(e) + "\n")
            exceptions.append(files[i])
            # optional: delete local version of failed download
        finally:
            print('reached'+'\n')
    pass

    return missions_keys, exceptions

## 3 Corpus functions

# 3.1 corpus_raw_texts() copies a list of texts (specified in a links_index file) from a folder (usually texts_archive()) to another to be consider the main corpus raw text archive

def corpus_raw_texts(texts_archive = texts_path,
                     exceptions = texts_removed,
                     corpus_texts_archive = corpus_texts_path,
                     clean_destination = False):

    if clean_destination:
        to_clean = glob.glob(corpus_texts_archive+"/*")
        for i in to_clean:
            os.remove(i)

    texts = [f for r, d, f in os.walk(texts_archive)][0]
    corpus_texts = [text for text in texts if text not in exceptions]

    print('The corpus contains: '+ str(len(corpus_texts))+' documents')

    for text in corpus_texts:
        shutil.copy(texts_archive + text, corpus_texts_archive + text)

    return corpus_texts

# 3.2 corpus_create_ppkoc() uses metadata and raw corpus to create the Plain PKOC

def corpus_create_ppkoc(metadata = mykeys):

    ppkoc = {}

    files = [d for a,b,c,d in metadata]
    metadata_keys = [c for a,b,c,d in metadata]

    for i in range(0,len(files)):

        print(i+1,'/',len(files))

        f = open(files[i], 'r')
        report = f.read().replace('\n', ' ') # remove new lines
        report = report.replace('English', '') # remove English
        report = re.sub(r'Page [0-9]*', '', report) # remove page number
        report = re.sub(r'S/\d+/\d+', '', report)

        ppkoc[metadata_keys[i]] = report

    return ppkoc

def corpus_create_rpkoc(plain_corpus = ppkoc):

    rpkoc = {}
    stop_words = set(stopwords.words('english'))

    for a in plain_corpus:
        print(a)
        rpkoc[a] = ppkoc[a].translate(str.maketrans('', '', string.punctuation)) # remove punctuation from each document
        rpkoc[a] = word_tokenize(rpkoc[a])
        rpkoc[a] = [w for w in rpkoc[a] if not w in stop_words]
        rpkoc[a] =  ' '.join(rpkoc[a]).split()
        porter = PorterStemmer()
        rpkoc[a] = [porter.stem(w) for w in rpkoc[a]]
        rpkoc[a] = [w for w in rpkoc[a] if len(w)>1]
        rpkoc[a] = [w for w in rpkoc[a] if not any(c.isdigit() for c in w)]

    return rpkoc

def corpus_create_tpkoc(plain_corpus = ppkoc):

    tpkoc = {}

    for i in range(0,len(plain_corpus)):
        print(i+1,'/',len(plain_corpus))
        text = word_tokenize(plain_corpus[list(plain_corpus)[i]])
        tpkoc[list(plain_corpus)[i]] = nltk.pos_tag(text)

    return tpkoc


##################
### Execution ####
##################

### 1 Build the index
# links_index = build_index(save = True, index_archive = index_archive_url)

### 2 Import the index`
# index_title = "index_2019-07-17.txt"
# links_index = import_index(index_archive = index_path, index_name = index_title)

### 3 Download reports
# download_reports(index = links_index, driver = driver_path, doc = False, reports_archive = reports_path)

### 4 Check mismatch between index and downloaded reports
# match_index_reports(index = links_index, reports_archive = reports_path) # the difference is 4

### 5 At this point we convert files to txt in bash

## From BASH move to the folder where you have the pdf and run the following line:

# for file in *.pdf; do pdftotext "$file"; done 
## Alternatively you can try to use the pdftotext package in python...

## Move things around
#source_files = "PKOC/Archive/reports/reports_archive/*.txt"
#target_folder = "PKOC/Archive/reports/texts_archive/"

#filelist = glob.glob(source_files)
#for single_file in filelist:
#    shutil.move(single_file, target_folder)

### 6 Filtering texts

## 6.1 Isolating conversion problems from reports archive (optional)
# isolated_reports = isolate_reports(reports_archive = reports_path,
#                                    texts_archive = texts_path,
#                                    isolated_reports_archive = isolated_reports_path,
#                                    min_length = 10, clean_destination = True, move = False, copy = True) # isolated 367

## 6.2 Isolating conversion problems from texts archive (optional)
# isolated_texts = isolate_texts(texts_archive = texts_path,
#                                isolated_texts_archive = isolated_texts_path,
#                                min_length = 10, clean_destination = True, move = False, copy = True) # isolated 367 Can we fix anything using docs?

## 6.3 Identify irrelevant texts (optional)
# positives, negatives = filter_non_missions_texts(texts_archive = texts_path,
#                                                  isolated_texts_archive = isolated_texts_path,
#                                                  irrelevant_texts_archive=irrelevant_texts_path,
#                                                  missions_info = missions_master_path,
#                                                  clean_destination = True, move = False, copy = True) # 1076 irrelevant

## 6.4 Create reports metadata. This function identifies text as in 6.2 and in 6.3 and store them in exc
# mykeys, exc = reports_metadata(texts_archive = texts_path,
#                                metadata_archive = metadata_path,
#                                missions_info = missions_master_path,
#                                missions_periods = missions_periods_path)
#pickle.dump(mykeys, open(metadata_path+"metadata.p", "wb"))
#
# Prepare metadata dataframe for R
#
# metadata_file = pickle.load(open(metadata_path+"metadata.p", "rb"))
# list_meta = []
# for file in metadata_file:
#     t = file[2].split('-')
#     t.append(metadata_file[0][3].split('/')[-1])
#     list_meta.append(t)
# with open("/home/ea/Documents/working_on/PKOC_2019/Archive/corpus/metadata/R_metadata.csv", "w", newline="") as f:
#     writer = csv.writer(f)
#    writer.writerows(list_meta)

### 7 Create corpora

## 7.1 Raw corpus
# exclusions = [a.split('/')[-1] for a in exc]
# corpus_raw_texts(texts_archive = texts_path,
#                  exceptions = exclusions,
#                  corpus_texts_archive = corpus_texts_path,
#                  clean_destination = True)

## 7.2 Plain PKOC
# ppkoc = corpus_create_ppkoc(metadata = mykeys)
# pickle.dump(ppkoc, open(corpus_dictionaries_path+"ppkoc.p", "wb"))

## 7.3 Reduced PKOC
# ppkoc = pickle.load( open(corpus_dictionaries_path+"ppkoc.p", "rb"))
# rpkoc = corpus_create_rpkoc(plain_corpus = ppkoc)
# pickle.dump(rpkoc, open(corpus_dictionaries_path+"rpkoc.p", "wb"))

## 7.4 Tagged PKOC
# tpkoc = corpus_create_tpkoc(plain_corpus = ppkoc)
# pickle.dump(tpkoc, open(corpus_dictionaries_path+"tpkoc.p", "wb"))
# tpkoc = pickle.load( open(corpus_dictionaries_path+"tpkoc.p", "rb"))
