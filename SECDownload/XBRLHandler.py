import os,sys,csv,time
import urllib.request as urllib2
from bs4 import BeautifulSoup
from time import sleep

class XBRLHandler(object):

    def __init__(
        self, xbrl_list_dir, xbrl_file_dir
    ):

        self.xbrl_list_dir = xbrl_list_dir
        self.xbrl_file_dir = xbrl_file_dir

    def CompanyCSVOpen(self): 
        CompanyListFile = os.path.expanduser(self.xbrl_list_dir)+'/CompanyListFile.csv'

        with open(CompanyListFile, "r") as f:
            csvReader = csv.reader(f, delimiter = ",")
            csvData = list(csvReader)
            return csvData
    
    def XbrlListCreate(self):
        XbrlListFile = os.path.expanduser(self.xbrl_list_dir)+'/XBRL_List.csv'

        if not os.path.isfile(XbrlListFile):
            with open(XbrlListFile, "w", encoding = 'utf8') as f:
                csvWriter = csv.writer(f, quoting = csv.QUOTE_NONNUMERIC)
                csvWriter.writerow(["Ticker", "CompanyName", "SIC","FilingDate", "FilingURL", "Form10KName", "Form10KLink",
                    "Form10KXBRL", "Form10KXBRL_SCH", "Form10KXBRL_CAL", "Form10KXBRL_DEF", "Form10KXBRL_LAB",
                    "Form10KXBRL_PRE"])
        else:
            print('XBRL List file already exists')
            pass

        return XbrlListFile
        
    def getXBRL_Link(
        self, Ticker, Name, FilingDate, FilingURL, FormType
        ):

        XbrlListFile = os.path.expanduser(self.xbrl_list_dir)+'/XBRL_List.csv'

        with open(XbrlListFile, "a") as f:
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


    def Para_loop(self, Ticker, Name, FilingDate, FilingURL, FormType):

        print(Name)

        self.getXBRL_Link(Ticker, Name, FilingDate, FilingURL, FormType)










