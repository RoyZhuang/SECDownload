import edgar
import os, sys, csv, time #"time" helps to break for the url visiting 
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import glob
import urllib.request as urllib2


class EdgarHandler(object):

    def __init__(
        self, edgar_dir, cik_dir, cik_merged_dir, start_year=None
    ):

        self.edgar_dir = edgar_dir
        self.cik_dir = cik_dir
        self.cik_merged_dir = cik_merged_dir
        self.start_year = start_year

    def GetEdgarIndex(self):
        
        edgarpath =  os.path.expanduser(self.edgar_dir)
        year = self.start_year # need to set up data type check
        try:
            edgar.download_index(edgarpath, year)
        except Exception as err:
            print('Exception:', str(err))

    def CIKDownload(self):
        edgarpath = os.path.expanduser(self.edgar_dir)
        cikpath = os.path.expanduser(self.cik_dir)
        NameOfCol = ['CIK', 'CompanyName', 'FilingType', 'FilingDate', 'FilingTxT', 'FilingIndex']
        SEC = 'https://www.sec.gov/Archives/'

        os.chdir(edgarpath) # could try another way
        edgar_list = [f for f in glob.glob("*.tsv")]
        print(edgar_list)

        for file in edgar_list:

            print(file)
            os.chdir(edgarpath) # could try another way
            edgar = pd.read_csv(file, names = NameOfCol, delimiter = '|')

            CIK_Data = edgar.loc[edgar.FilingType == '10-K']
            CIK_Data.reset_index(inplace = True)

            CIK_Data['FilingURL'] = CIK_Data.FilingIndex.apply(lambda x: SEC + x)

            os.chdir(cikpath)

            CIK = CIK_Data['CIK']
            CompanyName = CIK_Data['CompanyName']
            FilingURL = CIK_Data['FilingURL']
            FilingDate = CIK_Data['FilingDate']

            CIK_Database = {'Ticker': CIK, 'Name': CompanyName, 'FilingDate': FilingDate, 'FilingURL': FilingURL}

            CIK_Database = pd.DataFrame(CIK_Database)
            
            FileName = file.split('.')[0]
            
            SaveName = 'CIK_{}.csv'.format(FileName)

            CIK_Database.to_csv(SaveName, encoding='utf-8', index=False)


    def CIKMerge(self):
        cikpath = os.path.expanduser(self.cik_dir)
        cikmpath = os.path.expanduser(self.cik_merged_dir)
        
        os.chdir(cikpath)
        CIK_list = [f for f in glob.glob("*.csv")]
        CompanyListFile = pd.DataFrame()

        for file in CIK_list:
            Company = pd.read_csv(file, sep=",")
            CompanyListFile = pd.concat([CompanyListFile, Company])


        CompanyListFile = CompanyListFile.drop_duplicates(keep=False)
        filename = cikmpath+'/CompanyListFile.csv'
        CompanyListFile = CompanyListFile.sort_values(by=['Ticker', 'FilingDate'])
        CompanyListFile.to_csv(filename, encoding='utf-8', index=False)

            

            





        
        
