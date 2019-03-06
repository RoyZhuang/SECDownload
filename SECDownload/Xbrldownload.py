'''Download XBRL list of each company'''

import os,sys,csv,time
import urllib.request as urllib2
from bs4 import BeautifulSoup

from time import sleep
from joblib import Parallel, delayed
import multiprocessing

os.chdir('/Users/robertchuang/Stock-Prediction---Fscores/Data/XBRL/XBRL')
CompanyListFile = "/Users/robertchuang/Stock-Prediction---Fscores/Data/XBRL/CompanyListFile.csv"
Form10kListFile = "XBRL_List.csv"
num_cores = multiprocessing.cpu_count()  


with open(Form10kListFile, 'w', encoding= 'utf8') as f:
    csvWriter = csv.writer(f, quoting = csv.QUOTE_NONNUMERIC)
    csvWriter.writerow(["Ticker", "CompanyName", "SIC","FilingDate", "FilingURL", "Form10KName", "Form10KLink",
                        "Form10KXBRL", "Form10KXBRL_SCH", "Form10KXBRL_CAL", "Form10KXBRL_DEF", "Form10KXBRL_LAB",
                       "Form10KXBRL_PRE"])
        
def getXBRL_Link(Ticker, Name, FilingDate, FilingURL, FormType):
    
    with open(Form10kListFile, "a") as f:
        csvWriter = csv.writer(f, quoting = csv.QUOTE_NONNUMERIC)
        
        pageRequest = urllib2.Request(FilingURL)
        
        try:
            pageOpen = urllib2.urlopen(pageRequest)
        except URLError as err:
            time.sleep(5)
            print('Pause 5s to reopen :', FilingURL)
            pageOpen = urllib2.urlopen(pageRequest)
        except:
            print('Something goes wrong')
            pass
        
        pageRead = pageOpen.read()
        
        soup = BeautifulSoup(pageRead,"html.parser")

        try:
            table = soup.find("table", { "summary" : "Document Format Files" })
        except:
            print('Cannot find table for: ', Name, FilingURL)
                
        try:
            CompanyInfo = soup.find("div", { "class" : "companyInfo" })
            SIC = CompanyInfo.find("b").text
            
        except:
            print('Cannot find SIC for: ', Name, FilingURL)
            SIC = "NA"
        
        xbrl_link = ''
        xbrl_sch_link = ''
        xbrl_cal_link = ''
        xbrl_def_link = ''
        xbrl_lab_link = ''
        xbrl_pre_link = ''
        table_tag = soup.find('table', class_='tableFile', summary='Data Files')
        
        try:
            rows = table_tag.find_all('tr')
            for row in rows:
                cells = row.find_all('td')
                if len(cells) > 3:
                    if 'INS' in cells[3].text:
                        xbrl_link = 'https://www.sec.gov' + cells[2].a['href']
                    if 'SCH' in cells[3].text:
                        xbrl_sch_link = 'https://www.sec.gov' + cells[2].a['href']
                    if 'CAL' in cells[3].text:
                        xbrl_cal_link = 'https://www.sec.gov' + cells[2].a['href']
                    if 'DEF' in cells[3].text:
                        xbrl_def_link = 'https://www.sec.gov' + cells[2].a['href']
                    if 'LAB' in cells[3].text:
                        xbrl_lab_link = 'https://www.sec.gov' + cells[2].a['href']
                    if 'PRE' in cells[3].text:
                        xbrl_pre_link = 'https://www.sec.gov' + cells[2].a['href']
                        
        except:
            pass
        
        
        for row in table.findAll("tr"):
            cells = row.findAll("td")
            if len(cells)==5:
                if cells[3].text.strip() == FormType:
                    link = cells[2].find("a")
                    formLink = "https://www.sec.gov"+link['href']
                    formName = link.text.encode('utf8').strip()
                    
                    
                    csvWriter.writerow([Ticker, Name, SIC, FilingDate, FilingURL, 
                                        str(formName, encoding = 'utf-8'), formLink, 
                                       xbrl_link, xbrl_sch_link, xbrl_cal_link,
                                        xbrl_def_link, xbrl_lab_link, xbrl_pre_link])

def Para_loop(Ticker, Name, FilingDate, FilingURL, FormType):

    print(Name)

    getXBRL_Link(Ticker, Name, FilingDate, FilingURL, FormType)

def main():
    
    FormType = "10-K"
    nbDocPause = 10 ### <=== Type your number of documents to download in one batch
    nbSecPause = 1 ### <=== Type your pausing time in seconds between each batch
    
    with open(CompanyListFile, "r") as f:
        csvReader = csv.reader(f, delimiter = ",")
        csvData = list(csvReader)
    
    with open(Form10kListFile, "a") as f:
        csvWriter = csv.writer(f, quoting = csv.QUOTE_NONNUMERIC)
        
        
        Parallel(n_jobs=num_cores, prefer="threads")(delayed(Para_loop)(Ticker = rowData[0],
                                                      Name = rowData[1], FilingDate = rowData[2],
                                                     FilingURL = rowData[3], FormType = FormType) for rowData in csvData[1:])

tStart = time.time()


if __name__ == "__main__":
    main()

tEnd = time.time()
print("It cost %f sec" % (tEnd - tStart))



