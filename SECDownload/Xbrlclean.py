
import pandas as pd

XBRL_list = pd.read_csv("/Users/robertchuang/Stock-Prediction---Fscores/Data/XBRL/XBRL_List.csv")
XBRL_list_clean = XBRL_list.dropna()

XBRL_list_clean['Symbol'] = [os.path.basename(x).split('-')[0] for x in XBRL_list_clean['Form10KXBRL']]

cols = XBRL_list_clean.columns.tolist()
cols = cols[-1:] + cols[:-1]
XBRL_list_clean = XBRL_list_clean[cols]
XBRL_list_clean


path = '/Users/robertchuang/Stock-Prediction---Fscores/Data/XBRL/'

filename = path + 'XBRL_List_clean.csv'

XBRL_list_clean.to_csv(filename, encoding='utf-8', index=False)



