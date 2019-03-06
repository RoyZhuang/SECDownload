#! /usr/bin/Rscript

import subprocess

import os,sys,csv,time
import urllib.request as urllib2
from bs4 import BeautifulSoup
import pandas as pd

from time import sleep
from joblib import Parallel, delayed
import multiprocessing

os.chdir('/Users/robertchuang/Stock-Prediction---Fscores/Data/XBRL/XBRLFile')

FormXBRLListFile = "/Users/robertchuang/Stock-Prediction---Fscores/Data/XBRL/XBRL_List_clean.csv" #a csv file (output of the 2Get10kLinks.py script) with the list of 10-K links
logFile = "/Users/robertchuang/Stock-Prediction---Fscores/Data/XBRL/XBRLDownloadLog.csv" #a csv file (output of the current script) with the download history of 10-K forms

num_cores = multiprocessing.cpu_count()  
Script_cmd = "Rscript /Users/robertchuang/Stock-Prediction---Fscores/Data/XBRL/XBRLFile/DownloadMySQL_3.R"


def Download_XBRL(links):
    
    for link in links:

        try:
            pageRequest = urllib2.Request(link)
            pageOpen = urllib2.urlopen(pageRequest)
            pageRead = pageOpen.read()

            xmlname = link.split('/')[8]
            xmlfile = open(xmlname,'wb')
            xmlfile.write(pageRead)
            xmlfile.close()
        except:
            pass

csvData = pd.read_csv(FormXBRLListFile)

def Para_loop(Symbol, Ticker, Name, XBRL, XBRL_SCH, XBRL_CAL, XBRL_DEF, XBRL_LAB, XBRL_PRE):

    try:
        link = [XBRL, XBRL_SCH, XBRL_CAL, XBRL_DEF, XBRL_LAB, XBRL_PRE]
        Download_XBRL(link)

        Download = 'Success'
        
        try:
            
            bname = os.path.basename(XBRL)
            cmd = Script_cmd + ' ' + Symbol + ' ' + str(Ticker) + ' ' + bname
            subprocess.call(cmd, shell=True)
                        
            Parse = 'Success'
            
        except:
            
            Parse = 'Fail '

        RemoveFile = [os.path.basename(XBRL), os.path.basename(XBRL_SCH), os.path.basename(XBRL_CAL),
                     os.path.basename(XBRL_DEF), os.path.basename(XBRL_LAB), os.path.basename(XBRL_PRE)]
        
        for rmfile in RemoveFile:
            os.remove(rmfile)
        

    except:
        Download = 'Fail'
        
    csvWriter.writerow([Symbol, Ticker, Name, Download, Parse])

tStart = time.time()

with open(logFile, "w") as f:
    
    csvWriter = csv.writer(f, quoting = csv.QUOTE_NONNUMERIC)

    if not os.path.isfile(logFile):
        csvWriter.writerow(['Symbol', 'Ticker', 'CompanyName', 'Download', 'Parse'])
    else:
        pass
    
    
    Parallel(n_jobs=num_cores, prefer="threads")(delayed(Para_loop)(Symbol = rowData[0], Ticker = int(rowData[1]),
                                              Name = rowData[2], XBRL = rowData[8],
                                             XBRL_SCH = rowData[9], XBRL_CAL = rowData[10],
                                            XBRL_DEF = rowData[11], XBRL_LAB = rowData[12],
                                            XBRL_PRE = rowData[13]) for rowData in csvData[:20].values)
        

tEnd = time.time()
print("It cost %f sec" % (tEnd - tStart))
