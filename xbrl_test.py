import datetime

from SECDownload import settings, XBRLHandler
from joblib import Parallel, delayed
import multiprocessing



if __name__ == "__main__":
    # Configuration data
    testing = False
    config = settings.from_file(
        settings.DEFAULT_CONFIG_FILENAME, testing
    )

    print(config)
    
    test = XBRLHandler.XBRLHandler(
        xbrl_list_dir = config.XBRL_LIST_DIR,
        xbrl_file_dir = config.XBRL_FILE_DIR
        )

    csvData = test.CompanyCSVOpen()
    XbrlListFile = test.XbrlListCreate()
    FormType = "10-K"
    num_cores = multiprocessing.cpu_count()  

    Parallel(n_jobs=num_cores, prefer="threads")(delayed(test.Para_loop)(Ticker = rowData[0],
                                                Name = rowData[1], FilingDate = rowData[2],
                                                FilingURL = rowData[3], FormType = FormType) for rowData in csvData[1:])

    
