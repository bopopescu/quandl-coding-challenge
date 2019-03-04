import numpy as np
import pandas as pd
import pandas_datareader.data as web
import quandl
import pickle
import sqlalchemy
import datetime
import matplotlib.pyplot as plt
from matplotlib import style

style.use('fivethirtyeight')
import mysql.connector
import math

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="pa55word",
    database="esdb",
)

print(mydb)

# my_cursor = mydb.cursor()
# my_cursor.execute("CREATE TABLE es1 (Open FLOAT(10), High FLOAT(10), Low FLOAT(10), Last FLOAT(10), Change FLOAT(10), Settle FLOAT(10), Volume FLOAT(10), Previous_Day_Open_Interest FLOAT(10), Date DATE PRIMARY Key)")
# my_cursor.execute("SHOW TABLES")
# for table in my_cursor:
#     print(table)


# my_cursor.execute("CREATE DATABASE testdb")

# my_cursor.execute("SHOW DATABASES")
# for db in my_cursor:
#     print(db[0])

# my_cursor.execute("CREATE TABLE users (name VARCHAR(255), email VARCHAR(255), age INTEGER(10), user_id INTEGER AUTO_INCREMENT PRIMARY Key)")
# my_cursor.execute("SHOW TABLES")
# for table in my_cursor:
#     print(table[0])


# ES1 = quandl.get("CHRIS/CME_ES1", authtoken="_RagzvAdXfcfbs6hvGK7")
# ES2 = quandl.get("CHRIS/CME_ES2", authtoken="_RagzvAdXfcfbs6hvGK7")
# ES3 = quandl.get("CHRIS/CME_ES3", authtoken="_RagzvAdXfcfbs6hvGK7")
# ES4 = quandl.get("CHRIS/CME_ES4", authtoken="_RagzvAdXfcfbs6hvGK7")
# NQ1 = quandl.get("CHRIS/CME_NQ1", authtoken="_RagzvAdXfcfbs6hvGK7")
# NQ2 = quandl.get("CHRIS/CME_NQ2", authtoken="_RagzvAdXfcfbs6hvGK7")
# CL1 = quandl.get("CHRIS/CME_CL1", authtoken="_RagzvAdXfcfbs6hvGK7")
# CL2 = quandl.get("CHRIS/CME_CL2", authtoken="_RagzvAdXfcfbs6hvGK7")
# CL3 = quandl.get("CHRIS/CME_CL3", authtoken="_RagzvAdXfcfbs6hvGK7")
# CL4 = quandl.get("CHRIS/CME_CL4", authtoken="_RagzvAdXfcfbs6hvGK7")
# NG1 = quandl.get("CHRIS/CME_NG1", authtoken="_RagzvAdXfcfbs6hvGK7")
# NG2 = quandl.get("CHRIS/CME_NG2", authtoken="_RagzvAdXfcfbs6hvGK7")
# NG3 = quandl.get("CHRIS/CME_NG3", authtoken="_RagzvAdXfcfbs6hvGK7")
# NG4 = quandl.get("CHRIS/CME_NG4", authtoken="_RagzvAdXfcfbs6hvGK7")
# GC1 = quandl.get("CHRIS/CME_GC1", authtoken="_RagzvAdXfcfbs6hvGK7")
# GC2 = quandl.get("CHRIS/CME_GC2", authtoken="_RagzvAdXfcfbs6hvGK7")
# GC3 = quandl.get("CHRIS/CME_GC3", authtoken="_RagzvAdXfcfbs6hvGK7")
# GC4 = quandl.get("CHRIS/CME_GC4", authtoken="_RagzvAdXfcfbs6hvGK7")

# my_cursor = mydb.cursor()
# my_cursor.execute('CREATE TABLE es1 (ES1)')
# my_cursor.execute("SHOW TABLES")
# for table in my_cursor:
#     print(table[0])


# print(ES2.head())


# ES2 to CSV
# ES1.to_csv('ES1.csv')
# ES2.to_csv('ES2.csv')
# ES3.to_csv('ES3.csv')
# ES4.to_csv('ES4.csv')
# NQ1.to_csv('NQ1.csv')
# NQ2.to_csv('NQ2.csv')
# CL1.to_csv('CL1.csv')
# CL2.to_csv('CL2.csv')
# CL3.to_csv('CL3.csv')
# CL4.to_csv('CL4.csv')
# NG1.to_csv('NG1.csv')
# NG2.to_csv('NG2.csv')
# NG3.to_csv('NG3.csv')
# NG4.to_csv('NG4.csv')
# GC1.to_csv('GC1.csv')
# GC2.to_csv('GC2.csv')
# GC3.to_csv('GC3.csv')
# GC4.to_csv('GC4.csv')

# ES2 to pickle
# ES1df = pd.read_csv('ES1.csv')
# ES2df = pd.read_csv('ES2.csv')
# ES3df = pd.read_csv('ES3.csv')
# ES4df = pd.read_csv('ES4.csv')
# NQ1df = pd.read_csv('NQ1.csv')
# NQ2df = pd.read_csv('NQ2.csv')
# CL1df = pd.read_csv('CL1.csv')
# CL2df = pd.read_csv('CL2.csv')
# CL3df = pd.read_csv('CL3.csv')
# CL4df = pd.read_csv('CL4.csv')
# NG1df = pd.read_csv('NG1.csv')
# NG2df = pd.read_csv('NG2.csv')
# NG3df = pd.read_csv('NG3.csv')
# NG4df = pd.read_csv('NG4.csv')
# GC1df = pd.read_csv('GC1.csv')
# GC2df = pd.read_csv('GC2.csv')
# GC3df = pd.read_csv('GC3.csv')
# GC4df = pd.read_csv('GC4.csv')

# with open('ES1.pkl', 'wb') as pickle_file:
#     pickle.dump(ES1df, pickle_file)
#
# with open('ES1.pkl', 'rb') as pickle_file:
#     ES1_pickle = pickle.load(pickle_file)
#
# with open('ES2.pkl', 'wb') as pickle_file:
#     pickle.dump(ES2df, pickle_file)
#
# with open('ES2.pkl', 'rb') as pickle_file:
#     ES2_pickle = pickle.load(pickle_file)
#
# with open('ES3.pkl', 'wb') as pickle_file:
#     pickle.dump(ES3df, pickle_file)
#
# with open('ES3.pkl', 'rb') as pickle_file:
#     ES3_pickle = pickle.load(pickle_file)
#
# with open('ES4.pkl', 'wb') as pickle_file:
#     pickle.dump(ES4df, pickle_file)
#
# with open('ES4.pkl', 'rb') as pickle_file:
#     ES4_pickle = pickle.load(pickle_file)

# print(ES4_pickle.head())

# with open('NQ1.pkl', 'wb') as pickle_file:
#     pickle.dump(NQ1df, pickle_file)
#
# with open('NQ1.pkl', 'rb') as pickle_file:
#     NQ1_pickle = pickle.load(pickle_file)
#
# with open('NQ2.pkl', 'wb') as pickle_file:
#     pickle.dump(NQ2df, pickle_file)
#
# with open('NQ2.pkl', 'rb') as pickle_file:
#     NQ2_pickle = pickle.load(pickle_file)
#
# with open('CL1.pkl', 'wb') as pickle_file:
#     pickle.dump(CL1df, pickle_file)
#
# with open('CL1.pkl', 'rb') as pickle_file:
#     CL1_pickle = pickle.load(pickle_file)
#
# with open('CL2.pkl', 'wb') as pickle_file:
#     pickle.dump(CL2df, pickle_file)
#
# with open('CL2.pkl', 'rb') as pickle_file:
#     CL2_pickle = pickle.load(pickle_file)
#
# with open('CL3.pkl', 'wb') as pickle_file:
#     pickle.dump(CL3df, pickle_file)
#
# with open('CL3.pkl', 'rb') as pickle_file:
#     CL3_pickle = pickle.load(pickle_file)
#
# with open('CL4.pkl', 'wb') as pickle_file:
#     pickle.dump(CL4df, pickle_file)
#
# with open('CL4.pkl', 'rb') as pickle_file:
#     CL4_pickle = pickle.load(pickle_file)

# with open('NG1.pkl', 'wb') as pickle_file:
#     pickle.dump(NG1df, pickle_file)
#
# with open('NG1.pkl', 'rb') as pickle_file:
#     NG1_pickle = pickle.load(pickle_file)
#
# with open('NG2.pkl', 'wb') as pickle_file:
#     pickle.dump(NG2df, pickle_file)
#
# with open('NG2.pkl', 'rb') as pickle_file:
#     NG2_pickle = pickle.load(pickle_file)
#
# with open('NG3.pkl', 'wb') as pickle_file:
#     pickle.dump(NG3df, pickle_file)
#
# with open('NG3.pkl', 'rb') as pickle_file:
#     NG3_pickle = pickle.load(pickle_file)
#
# with open('NG4.pkl', 'wb') as pickle_file:
#     pickle.dump(NG4df, pickle_file)
#
# with open('NG4.pkl', 'rb') as pickle_file:
#     NG4_pickle = pickle.load(pickle_file)
#
# with open('GC1.pkl', 'wb') as pickle_file:
#     pickle.dump(GC1df, pickle_file)
#
# with open('GC1.pkl', 'rb') as pickle_file:
#     GC1_pickle = pickle.load(pickle_file)
#
# with open('GC2.pkl', 'wb') as pickle_file:
#     pickle.dump(GC2df, pickle_file)
#
# with open('GC2.pkl', 'rb') as pickle_file:
#     GC2_pickle = pickle.load(pickle_file)
#
# with open('GC3.pkl', 'wb') as pickle_file:
#     pickle.dump(GC3df, pickle_file)
#
# with open('GC3.pkl', 'rb') as pickle_file:
#     GC3_pickle = pickle.load(pickle_file)
#
# with open('GC4.pkl', 'wb') as pickle_file:
#     pickle.dump(GC4df, pickle_file)
#
# with open('GC4.pkl', 'rb') as pickle_file:
#     GC4_pickle = pickle.load(pickle_file)

# print(NG4_pickle.head())

# OPEN PICKLE FILE
with open('ES1.pkl', 'rb') as pickle_file:
    ES1_table = pickle.load(pickle_file)

with open('ES2.pkl', 'rb') as pickle_file:
    ES2_table = pickle.load(pickle_file)

with open('ES3.pkl', 'rb') as pickle_file:
    ES3_table = pickle.load(pickle_file)

with open('ES4.pkl', 'rb') as pickle_file:
    ES4_table = pickle.load(pickle_file)

# print(ES1_table.head())
with open('CL1.pkl', 'rb') as pickle_file:
    CL1_table = pickle.load(pickle_file)

with open('CL2.pkl', 'rb') as pickle_file:
    CL2_table = pickle.load(pickle_file)

with open('CL3.pkl', 'rb') as pickle_file:
    CL3_table = pickle.load(pickle_file)

with open('CL4.pkl', 'rb') as pickle_file:
    CL4_table = pickle.load(pickle_file)

with open('GC1.pkl', 'rb') as pickle_file:
    GC1_table = pickle.load(pickle_file)

with open('GC2.pkl', 'rb') as pickle_file:
    GC2_table = pickle.load(pickle_file)

with open('GC3.pkl', 'rb') as pickle_file:
    GC3_table = pickle.load(pickle_file)

with open('GC4.pkl', 'rb') as pickle_file:
    GC4_table = pickle.load(pickle_file)

with open('NG1.pkl', 'rb') as pickle_file:
    NG1_table = pickle.load(pickle_file)

with open('NG2.pkl', 'rb') as pickle_file:
    NG2_table = pickle.load(pickle_file)

with open('NG3.pkl', 'rb') as pickle_file:
    NG3_table = pickle.load(pickle_file)

with open('NG4.pkl', 'rb') as pickle_file:
    NG4_table = pickle.load(pickle_file)

with open('NQ1.pkl', 'rb') as pickle_file:
    NQ1_table = pickle.load(pickle_file)

with open('NQ2.pkl', 'rb') as pickle_file:
    NQ2_table = pickle.load(pickle_file)

# print(ES1_table)

# TESTING FUNCTION - DOUBLING COLUMN
# ES4_table['Open_double'] = ES4_table['Open']*2
#
# print(ES4_table[['Open','Open_double']])


# ES1And2 = pd.merge(ES1, ES2, left_index=True, right_index=True)
# ES1Thru3 = pd.merge(ES1And2, ES3, left_index=True, right_index=True)
# ES1Thru4 = pd.merge(ES1Thru3, ES4, left_index=True, right_index=True)
# ES1Thru4.to_csv('ES1Thru4.csv')
#
# ESdf = pd.read_csv("ES1Thru4.csv")
# ESdf.to_sql(
#     name='ES_Contracts'
#     con=app.py,
#     index=True,
#     if_exists='replace'
# )

# my_cursor.execute("SELECT * from es1thru4sql")
# result = my_cursor.fetchall()
# for row in result:
#     print(row)


# CREATE PICKLE FILE
# with open('ES.pkl', 'wb') as pickle_file:
#     pickle.dump(ESdf, pickle_file)
#

# OPEN PICKLE FILE
# with open('ES2.pkl', 'rb') as pickle_file:
#     ES2_table = pickle.load(pickle_file)

ES1_table['ret'] = ES1_table['Settle'] / ES1_table['Settle'].shift(1) - 1
ES2_table['ret'] = ES2_table['Settle'] / ES2_table['Settle'].shift(1) - 1
ES3_table['ret'] = ES3_table['Settle'] / ES3_table['Settle'].shift(1) - 1
ES4_table['ret'] = ES4_table['Settle'] / ES4_table['Settle'].shift(1) - 1
CL1_table['ret'] = CL1_table['Settle'] / CL1_table['Settle'].shift(1) - 1
CL2_table['ret'] = CL2_table['Settle'] / CL2_table['Settle'].shift(1) - 1
CL3_table['ret'] = CL3_table['Settle'] / CL3_table['Settle'].shift(1) - 1
CL4_table['ret'] = CL4_table['Settle'] / CL4_table['Settle'].shift(1) - 1
GC1_table['ret'] = GC1_table['Settle'] / GC1_table['Settle'].shift(1) - 1
GC2_table['ret'] = GC2_table['Settle'] / GC2_table['Settle'].shift(1) - 1
GC3_table['ret'] = GC3_table['Settle'] / GC3_table['Settle'].shift(1) - 1
GC4_table['ret'] = GC4_table['Settle'] / GC4_table['Settle'].shift(1) - 1
NG1_table['ret'] = NG1_table['Settle'] / NG1_table['Settle'].shift(1) - 1
NG2_table['ret'] = NG2_table['Settle'] / NG2_table['Settle'].shift(1) - 1
NG3_table['ret'] = NG3_table['Settle'] / NG3_table['Settle'].shift(1) - 1
NG4_table['ret'] = NG4_table['Settle'] / NG4_table['Settle'].shift(1) - 1
NQ1_table['ret'] = NQ1_table['Settle'] / NQ1_table['Settle'].shift(1) - 1
NQ2_table['ret'] = NQ2_table['Settle'] / NQ2_table['Settle'].shift(1) - 1

#Calculating Volatility

ES1_table['volatility'] = ES1_table['ret'].std()
ES2_table['volatility'] = ES2_table['ret'].std()
ES3_table['volatility'] = ES3_table['ret'].std()
ES4_table['volatility'] = ES4_table['ret'].std()
CL1_table['volatility'] = CL1_table['ret'].std()
CL2_table['volatility'] = CL2_table['ret'].std()
CL3_table['volatility'] = CL3_table['ret'].std()
CL4_table['volatility'] = CL4_table['ret'].std()
GC1_table['volatility'] = GC1_table['ret'].std()
GC2_table['volatility'] = GC2_table['ret'].std()
GC3_table['volatility'] = GC3_table['ret'].std()
GC4_table['volatility'] = GC4_table['ret'].std()
NG1_table['volatility'] = NG1_table['ret'].std()
NG2_table['volatility'] = NG2_table['ret'].std()
NG3_table['volatility'] = NG3_table['ret'].std()
NG4_table['volatility'] = NG4_table['ret'].std()
NQ1_table['volatility'] = NQ1_table['ret'].std()
NQ2_table['volatility'] = NQ2_table['ret'].std()

#1) Get Annualized Volatility of Returns

ES1_table['ann_vol'] = ES1_table[['volatility']] * math.sqrt(252)
ES2_table['ann_vol'] = ES2_table[['volatility']] * math.sqrt(252)
ES3_table['ann_vol'] = ES3_table[['volatility']] * math.sqrt(252)
ES4_table['ann_vol'] = ES4_table[['volatility']] * math.sqrt(252)
CL1_table['ann_vol'] = CL1_table[['volatility']] * math.sqrt(252)
CL2_table['ann_vol'] = CL2_table[['volatility']] * math.sqrt(252)
CL3_table['ann_vol'] = CL3_table[['volatility']] * math.sqrt(252)
CL4_table['ann_vol'] = CL4_table[['volatility']] * math.sqrt(252)
GC1_table['ann_vol'] = GC1_table[['volatility']] * math.sqrt(252)
GC2_table['ann_vol'] = GC2_table[['volatility']] * math.sqrt(252)
GC3_table['ann_vol'] = GC3_table[['volatility']] * math.sqrt(252)
GC4_table['ann_vol'] = GC4_table[['volatility']] * math.sqrt(252)
NG1_table['ann_vol'] = NG1_table[['volatility']] * math.sqrt(252)
NG2_table['ann_vol'] = NG2_table[['volatility']] * math.sqrt(252)
NG3_table['ann_vol'] = NG3_table[['volatility']] * math.sqrt(252)
NG4_table['ann_vol'] = NG4_table[['volatility']] * math.sqrt(252)
NQ1_table['ann_vol'] = NQ1_table[['volatility']] * math.sqrt(252)
NQ2_table['ann_vol'] = NQ2_table[['volatility']] * math.sqrt(252)

ES_annual_vol = pd.DataFrame(
    [['CME_ES1', ES1_table['ann_vol'].max()],
     ['CME_ES2', ES2_table['ann_vol'].max()],
     ['CME_ES3', ES3_table['ann_vol'].max()],
     ['CME_ES4', ES4_table['ann_vol'].max()]],
    index=None,
    columns=['code_name', 'annualized_vol']
)

CL_annual_vol = pd.DataFrame(
    [['CME_CL1', CL1_table['ann_vol'].max()],
     ['CME_CL2', CL2_table['ann_vol'].max()],
     ['CME_CL3', CL3_table['ann_vol'].max()],
     ['CME_CL4', CL4_table['ann_vol'].max()]],
    index=None,
    columns=['code_name', 'annualized_vol']
)

GC_annual_vol = pd.DataFrame(
    [['CME_GC1', GC1_table['ann_vol'].max()],
     ['CME_GC2', GC2_table['ann_vol'].max()],
     ['CME_GC3', GC3_table['ann_vol'].max()],
     ['CME_GC4', GC4_table['ann_vol'].max()]],
    index=None,
    columns=['code_name', 'annualized_vol']
)

NG_annual_vol = pd.DataFrame(
    [['CME_NG1', NG1_table['ann_vol'].max()],
     ['CME_NG2', NG2_table['ann_vol'].max()],
     ['CME_NG3', NG3_table['ann_vol'].max()],
     ['CME_NG4', NG4_table['ann_vol'].max()]],
    index=None,
    columns=['code_name', 'annualized_vol']
)

NQ_annual_vol = pd.DataFrame(
    [['CME_NQ1', NQ1_table['ann_vol'].max()],
     ['CME_NQ2', NQ2_table['ann_vol'].max()]],
    index=None,
    columns=['code_name', 'annualized_vol']
)

def CME_ES_annual_vol():
    print(ES_annual_vol.sort_values('annualized_vol', ascending=False))

def CME_CL_annual_vol():
    print(CL_annual_vol.sort_values('annualized_vol', ascending=False))

def CME_GC_annual_vol():
    print(GC_annual_vol.sort_values('annualized_vol', ascending=False))

def CME_NG_annual_vol():
    print(NG_annual_vol.sort_values('annualized_vol', ascending=False))

def CME_NQ_annual_vol():
    print(NQ_annual_vol.sort_values('annualized_vol', ascending=False))


print('\n\n1) Get Annualized Volatility of Returns')
function_dict = {'CME_ES': CME_ES_annual_vol, 'CME_CL': CME_CL_annual_vol, 'CME_GC': CME_GC_annual_vol,
                 'CME_NG': CME_NG_annual_vol, 'CME_NQ': CME_NQ_annual_vol}
func = input('Please type a contract root to see its annualized volatility (Ex. CME_ES)\nCONTRACT ROOT:\n')
function_dict[func]()

#2. Get Trailing 1 year volatility

ES1_table['tr_1_yr_vol'] = ES1_table[['ret']].rolling(252).std()
ES2_table['tr_1_yr_vol'] = ES2_table[['ret']].rolling(252).std()
ES3_table['tr_1_yr_vol'] = ES3_table[['ret']].rolling(252).std()
ES4_table['tr_1_yr_vol'] = ES4_table[['ret']].rolling(252).std()
CL1_table['tr_1_yr_vol'] = CL1_table[['ret']].rolling(252).std()
CL2_table['tr_1_yr_vol'] = CL2_table[['ret']].rolling(252).std()
CL3_table['tr_1_yr_vol'] = CL3_table[['ret']].rolling(252).std()
CL4_table['tr_1_yr_vol'] = CL4_table[['ret']].rolling(252).std()
GC1_table['tr_1_yr_vol'] = GC1_table[['ret']].rolling(252).std()
GC2_table['tr_1_yr_vol'] = GC2_table[['ret']].rolling(252).std()
GC3_table['tr_1_yr_vol'] = GC3_table[['ret']].rolling(252).std()
GC4_table['tr_1_yr_vol'] = GC4_table[['ret']].rolling(252).std()
NG1_table['tr_1_yr_vol'] = NG1_table[['ret']].rolling(252).std()
NG2_table['tr_1_yr_vol'] = NG2_table[['ret']].rolling(252).std()
NG3_table['tr_1_yr_vol'] = NG3_table[['ret']].rolling(252).std()
NG4_table['tr_1_yr_vol'] = NG4_table[['ret']].rolling(252).std()
NQ1_table['tr_1_yr_vol'] = NQ1_table[['ret']].rolling(252).std()
NQ2_table['tr_1_yr_vol'] = NQ2_table[['ret']].rolling(252).std()


ES_tr_1_yr_vol = pd.DataFrame(
    [['CME_ES1', ES1_table['tr_1_yr_vol'].mean()],
     ['CME_ES2', ES2_table['tr_1_yr_vol'].mean()],
     ['CME_ES3', ES3_table['tr_1_yr_vol'].mean()],
     ['CME_ES4', ES4_table['tr_1_yr_vol'].mean()]],
    index=None,
    columns=['code_name', 'tr_1_yr_vol']
)

CL_tr_1_yr_vol = pd.DataFrame(
    [['CME_CL1', CL1_table['tr_1_yr_vol'].mean()],
     ['CME_CL2', CL2_table['tr_1_yr_vol'].mean()],
     ['CME_CL3', CL3_table['tr_1_yr_vol'].mean()],
     ['CME_CL4', CL4_table['tr_1_yr_vol'].mean()]],
    index=None,
    columns=['code_name', 'tr_1_yr_vol']
)

GC_tr_1_yr_vol = pd.DataFrame(
    [['CME_GC1', GC1_table['tr_1_yr_vol'].mean()],
     ['CME_GC2', GC2_table['tr_1_yr_vol'].mean()],
     ['CME_GC3', GC3_table['tr_1_yr_vol'].mean()],
     ['CME_GC4', GC4_table['tr_1_yr_vol'].mean()]],
    index=None,
    columns=['code_name', 'tr_1_yr_vol']
)

NG_tr_1_yr_vol = pd.DataFrame(
    [['CME_NG1', NG1_table['tr_1_yr_vol'].mean()],
     ['CME_NG2', NG2_table['tr_1_yr_vol'].mean()],
     ['CME_NG3', NG3_table['tr_1_yr_vol'].mean()],
     ['CME_NG4', NG4_table['tr_1_yr_vol'].mean()]],
    index=None,
    columns=['code_name', 'tr_1_yr_vol']
)

NQ_tr_1_yr_vol = pd.DataFrame(
    [['CME_NQ1', NQ1_table['tr_1_yr_vol'].mean()],
     ['CME_NQ2', NQ2_table['tr_1_yr_vol'].mean()]],
    index=None,
    columns=['code_name', 'tr_1_yr_vol']
)

def CME_ES_tr_1_yr_vol():
    print(ES_tr_1_yr_vol.sort_values('tr_1_yr_vol', ascending=False))

def CME_CL_tr_1_yr_vol():
    print(CL_tr_1_yr_vol.sort_values('tr_1_yr_vol', ascending=False))

def CME_GC_tr_1_yr_vol():
    print(GC_tr_1_yr_vol.sort_values('tr_1_yr_vol', ascending=False))

def CME_NG_tr_1_yr_vol():
    print(NG_tr_1_yr_vol.sort_values('tr_1_yr_vol', ascending=False))

def CME_NQ_tr_1_yr_vol():
    print(NQ_tr_1_yr_vol.sort_values('tr_1_yr_vol', ascending=False))

print('\n\n2) Get Trailing One Year Volatility')
trailing_dict = {'CME_ES': CME_ES_tr_1_yr_vol, 'CME_CL': CME_CL_tr_1_yr_vol, 'CME_GC': CME_GC_tr_1_yr_vol,
                 'CME_NG': CME_NG_tr_1_yr_vol, 'CME_NQ': CME_NQ_tr_1_yr_vol}
trail = input('Please type a contract root to see its trailing one year volatility (Ex. CME_ES)\nCONTRACT ROOT:\n')
trailing_dict[trail]()

#3. Get Largest Single Day Return

ES1_max = ES1_table[ES1_table['ret']==ES1_table['ret'].max()]
ES2_max = ES2_table[ES2_table['ret']==ES2_table['ret'].max()]
ES3_max = ES3_table[ES3_table['ret']==ES3_table['ret'].max()]
ES4_max = ES4_table[ES4_table['ret']==ES4_table['ret'].max()]
CL1_max = CL1_table[CL1_table['ret']==CL1_table['ret'].max()]
CL2_max = CL2_table[CL2_table['ret']==CL2_table['ret'].max()]
CL3_max = CL3_table[CL3_table['ret']==CL3_table['ret'].max()]
CL4_max = CL4_table[CL4_table['ret']==CL4_table['ret'].max()]
GC1_max = GC1_table[GC1_table['ret']==GC1_table['ret'].max()]
GC2_max = GC2_table[GC2_table['ret']==GC2_table['ret'].max()]
GC3_max = GC3_table[GC3_table['ret']==GC3_table['ret'].max()]
GC4_max = GC4_table[GC4_table['ret']==GC4_table['ret'].max()]
NG1_max = NG1_table[NG1_table['ret']==NG1_table['ret'].max()]
NG2_max = NG2_table[NG2_table['ret']==NG2_table['ret'].max()]
NG3_max = NG3_table[NG3_table['ret']==NG3_table['ret'].max()]
NG4_max = NG4_table[NG4_table['ret']==NG4_table['ret'].max()]
NQ1_max = NQ1_table[NQ1_table['ret']==NQ1_table['ret'].max()]
NQ2_max = NQ2_table[NQ2_table['ret']==NQ2_table['ret'].max()]

ES_max_ret = pd.DataFrame(
    [[ES1_max['Date'].max(), 'CME_ES1', ES1_max['ret'].max()],
     [ES2_max['Date'].max(), 'CME_ES2', ES2_max['ret'].max()],
     [ES3_max['Date'].max(), 'CME_ES3', ES3_max['ret'].max()],
     [ES4_max['Date'].max(), 'CME_ES4', ES4_max['ret'].max()]],
    index=None,
    columns=['date', 'code_name', 'ret']
)
CL_max_ret = pd.DataFrame(
    [[CL1_max['Date'].max(), 'CME_CL1', CL1_max['ret'].max()],
     [CL2_max['Date'].max(), 'CME_CL2', CL2_max['ret'].max()],
     [CL3_max['Date'].max(), 'CME_CL3', CL3_max['ret'].max()],
     [CL4_max['Date'].max(), 'CME_CL4', CL4_max['ret'].max()]],
    index=None,
    columns=['date', 'code_name', 'ret']
)
GC_max_ret = pd.DataFrame(
    [[GC1_max['Date'].max(), 'CME_GC1', GC1_max['ret'].max()],
     [GC2_max['Date'].max(), 'CME_GC2', GC2_max['ret'].max()],
     [GC3_max['Date'].max(), 'CME_GC3', GC3_max['ret'].max()],
     [GC4_max['Date'].max(), 'CME_GC4', GC4_max['ret'].max()]],
    index=None,
    columns=['date', 'code_name', 'ret']
)
NG_max_ret = pd.DataFrame(
    [[NG1_max['Date'].max(), 'CME_NG1', NG1_max['ret'].max()],
     [NG2_max['Date'].max(), 'CME_NG2', NG2_max['ret'].max()],
     [NG3_max['Date'].max(), 'CME_NG3', NG3_max['ret'].max()],
     [NG4_max['Date'].max(), 'CME_NG4', NG4_max['ret'].max()]],
    index=None,
    columns=['date', 'code_name', 'ret']
)
NQ_max_ret = pd.DataFrame(
    [[NQ1_max['Date'].max(), 'CME_NQ1', NQ1_max['ret'].max()],
     [NQ2_max['Date'].max(), 'CME_NQ2', NQ2_max['ret'].max()]],
    index=None,
    columns=['date', 'code_name', 'ret']
)

def CME_ES_max_ret():
    print(ES_max_ret.sort_values('ret', ascending=False))

def CME_CL_max_ret():
    print(CL_max_ret.sort_values('ret', ascending=False))

def CME_GC_max_ret():
    print(GC_max_ret.sort_values('ret', ascending=False))

def CME_NG_max_ret():
    print(NG_max_ret.sort_values('ret', ascending=False))

def CME_NQ_max_ret():
    print(NQ_max_ret.sort_values('ret', ascending=False))

print('\n\n3) Get Largest Single Day Return')
single_day_dict = {'CME_ES': CME_ES_max_ret, 'CME_CL': CME_CL_max_ret, 'CME_GC': CME_GC_max_ret,
                 'CME_NG': CME_NG_max_ret, 'CME_NQ': CME_NQ_max_ret}
single = input('Please type a contract root to see its largest single day return (Ex. CME_ES)\nCONTRACT ROOT:\n')
single_day_dict[single]()


#4. Get Largest Annual Return
#Annual Return = (Settle[last] - Settle[first])/(number of indexes)  * 252

ES1_overall_gain = (ES1_table['Settle'].iat[-1])-(ES1_table['Settle'][0])
ES1_ann_gain = (ES1_overall_gain)/(len(ES1_table.index)) * 252
ES2_overall_gain = (ES2_table['Settle'].iat[-1])-(ES2_table['Settle'][0])
ES2_ann_gain = (ES2_overall_gain)/(len(ES2_table.index)) * 252
ES3_overall_gain = (ES3_table['Settle'].iat[-1])-(ES3_table['Settle'][0])
ES3_ann_gain = (ES3_overall_gain)/(len(ES3_table.index)) * 252
ES4_overall_gain = (ES4_table['Settle'].iat[-1])-(ES4_table['Settle'][0])
ES4_ann_gain = (ES4_overall_gain)/(len(ES4_table.index)) * 252

CL1_overall_gain = (CL1_table['Settle'].iat[-1])-(CL1_table['Settle'][0])
CL1_ann_gain = (CL1_overall_gain)/(len(CL1_table.index)) * 252
CL2_overall_gain = (CL2_table['Settle'].iat[-1])-(CL2_table['Settle'][0])
CL2_ann_gain = (CL2_overall_gain)/(len(CL2_table.index)) * 252
CL3_overall_gain = (CL3_table['Settle'].iat[-1])-(CL3_table['Settle'][0])
CL3_ann_gain = (CL3_overall_gain)/(len(CL3_table.index)) * 252
CL4_overall_gain = (CL4_table['Settle'].iat[-1])-(CL4_table['Settle'][0])
CL4_ann_gain = (CL4_overall_gain)/(len(CL4_table.index)) * 252

GC1_overall_gain = (GC1_table['Settle'].iat[-1])-(GC1_table['Settle'][0])
GC1_ann_gain = (GC1_overall_gain)/(len(GC1_table.index)) * 252
GC2_overall_gain = (GC2_table['Settle'].iat[-1])-(GC2_table['Settle'][0])
GC2_ann_gain = (GC2_overall_gain)/(len(GC2_table.index)) * 252
GC3_overall_gain = (GC3_table['Settle'].iat[-1])-(GC3_table['Settle'][0])
GC3_ann_gain = (GC3_overall_gain)/(len(GC3_table.index)) * 252
GC4_overall_gain = (GC4_table['Settle'].iat[-1])-(GC4_table['Settle'][0])
GC4_ann_gain = (GC4_overall_gain)/(len(GC4_table.index)) * 252

NG1_overall_gain = (NG1_table['Settle'].iat[-1])-(NG1_table['Settle'][0])
NG1_ann_gain = (NG1_overall_gain)/(len(NG1_table.index)) * 252
NG2_overall_gain = (NG2_table['Settle'].iat[-1])-(NG2_table['Settle'][0])
NG2_ann_gain = (NG2_overall_gain)/(len(NG2_table.index)) * 252
NG3_overall_gain = (NG3_table['Settle'].iat[-1])-(NG3_table['Settle'][0])
NG3_ann_gain = (NG3_overall_gain)/(len(NG3_table.index)) * 252
NG4_overall_gain = (NG4_table['Settle'].iat[-1])-(NG4_table['Settle'][0])
NG4_ann_gain = (NG4_overall_gain)/(len(NG4_table.index)) * 252

NQ1_overall_gain = (NQ1_table['Settle'].iat[-1])-(NQ1_table['Settle'][0])
NQ1_ann_gain = (NQ1_overall_gain)/(len(NQ1_table.index)) * 252
NQ2_overall_gain = (NQ2_table['Settle'].iat[-1])-(NQ2_table['Settle'][0])
NQ2_ann_gain = (NQ2_overall_gain)/(len(NQ2_table.index)) * 252

ES_ann_gain = pd.DataFrame(
    [['CME_ES1', ES1_ann_gain],
     ['CME_ES2', ES2_ann_gain],
     ['CME_ES3', ES3_ann_gain],
     ['CME_ES4', ES4_ann_gain]],
    index=None,
    columns=['code_name', 'annual_return']
)

CL_ann_gain = pd.DataFrame(
    [['CME_CL1', CL1_ann_gain],
     ['CME_CL2', CL2_ann_gain],
     ['CME_CL3', CL3_ann_gain],
     ['CME_CL4', CL4_ann_gain]],
    index=None,
    columns=['code_name', 'annual_return']
)

GC_ann_gain = pd.DataFrame(
    [['CME_GC1', GC1_ann_gain],
     ['CME_GC2', GC2_ann_gain],
     ['CME_GC3', GC3_ann_gain],
     ['CME_GC4', GC4_ann_gain]],
    index=None,
    columns=['code_name', 'annual_return']
)

NG_ann_gain = pd.DataFrame(
    [['CME_NG1', NG1_ann_gain],
     ['CME_NG2', NG2_ann_gain],
     ['CME_NG3', NG3_ann_gain],
     ['CME_NG4', NG4_ann_gain]],
    index=None,
    columns=['code_name', 'annual_return']
)

NQ_ann_gain = pd.DataFrame(
    [['CME_NQ1', NQ1_ann_gain],
     ['CME_NQ2', NQ2_ann_gain]],
    index=None,
    columns=['code_name', 'annual_return']
)

def CME_ES_ann_gain():
    print(ES_ann_gain.sort_values('annual_return', ascending=False))

def CME_CL_ann_gain():
    print(CL_ann_gain.sort_values('annual_return', ascending=False))

def CME_GC_ann_gain():
    print(GC_ann_gain.sort_values('annual_return', ascending=False))

def CME_NG_ann_gain():
    print(NG_ann_gain.sort_values('annual_return', ascending=False))

def CME_NQ_ann_gain():
    print(NQ_ann_gain.sort_values('annual_return', ascending=False))

print('\n\n4) Get Largest Annual Return')
ann_dict = {'CME_ES': CME_ES_ann_gain, 'CME_CL': CME_CL_ann_gain, 'CME_GC': CME_GC_ann_gain,
                 'CME_NG': CME_NG_ann_gain, 'CME_NQ': CME_NQ_ann_gain}
ann = input('Please type a contract root to see its annual return \nCONTRACT ROOT:\n')
ann_dict[ann]()

##Graphing with matplotlib

def plot_ES1():
    ES1_table['Settle'].plot(label='Settle Price', linewidth=1)
    plt.xlabel('index')
    plt.ylabel('Settle Price')
    plt.legend()
    plt.title('ES1 Price')
    plt.show()

def plot_ES2():
    ES2_table['Settle'].plot(label='Settle Price', linewidth=1)
    plt.legend()
    plt.show()

def plot_ES3():
    ES3_table['Settle'].plot(label='Settle Price', linewidth=1)
    plt.legend()
    plt.show()

def plot_ES4():
    ES4_table['Settle'].plot(label='Settle Price', linewidth=1)
    plt.legend()
    plt.show()

def plot_ES():
    plt.plot(ES1_table['Settle'], label='ES1', linewidth=1)
    plt.plot(ES2_table['Settle'], label='ES2', linewidth=1)
    plt.plot(ES3_table['Settle'], label='ES3', linewidth=1)
    plt.plot(ES4_table['Settle'], label='ES4', linewidth=1)
    plt.xlabel('index')
    plt.ylabel('Settle Price')
    plt.legend()
    plt.show()

def plot_CL1():
    CL1_table['Settle'].plot(label='Settle Price', linewidth=1)
    plt.xlabel('index')
    plt.ylabel('Settle Price')
    plt.legend()
    plt.title('CL1 Price')
    plt.show()

def plot_CL2():
    CL2_table['Settle'].plot(label='Settle Price', linewidth=1)
    plt.xlabel('index')
    plt.ylabel('Settle Price')
    plt.legend()
    plt.title('CL2 Price')
    plt.show()

def plot_CL3():
    CL3_table['Settle'].plot(label='Settle Price', linewidth=1)
    plt.xlabel('index')
    plt.ylabel('Settle Price')
    plt.legend()
    plt.title('CL3 Price')
    plt.show()

def plot_CL4():
    CL4_table['Settle'].plot(label='Settle Price', linewidth=1)
    plt.xlabel('index')
    plt.ylabel('Settle Price')
    plt.legend()
    plt.title('CL4 Price')
    plt.show()

def plot_CL():
    plt.plot(CL1_table['Settle'], label='CL1', linewidth=1)
    plt.plot(CL2_table['Settle'], label='CL2', linewidth=1)
    plt.plot(CL3_table['Settle'], label='CL3', linewidth=1)
    plt.plot(CL4_table['Settle'], label='CL4', linewidth=1)
    plt.xlabel('index')
    plt.ylabel('Settle Price')
    plt.legend()
    plt.show()

def plot_GC1():
    GC1_table['Settle'].plot(label='Settle Price', linewidth=1)
    plt.xlabel('index')
    plt.ylabel('Settle Price')
    plt.legend()
    plt.title('GC1 Price')
    plt.show()

def plot_GC2():
    GC2_table['Settle'].plot(label='Settle Price', linewidth=1)
    plt.xlabel('index')
    plt.ylabel('Settle Price')
    plt.legend()
    plt.title('GC2 Price')
    plt.show()

def plot_GC3():
    GC3_table['Settle'].plot(label='Settle Price', linewidth=1)
    plt.xlabel('index')
    plt.ylabel('Settle Price')
    plt.legend()
    plt.title('GC3 Price')
    plt.show()

def plot_GC4():
    GC4_table['Settle'].plot(label='Settle Price', linewidth=1)
    plt.xlabel('index')
    plt.ylabel('Settle Price')
    plt.legend()
    plt.title('GC4 Price')
    plt.show()

def plot_GC():
    plt.plot(GC1_table['Settle'], label='GC1', linewidth=1)
    plt.plot(GC2_table['Settle'], label='GC2', linewidth=1)
    plt.plot(GC3_table['Settle'], label='GC3', linewidth=1)
    plt.plot(GC4_table['Settle'], label='GC4', linewidth=1)
    plt.xlabel('index')
    plt.ylabel('Settle Price')
    plt.legend()
    plt.show()

def plot_NG1():
    NG1_table['Settle'].plot(label='Settle Price', linewidth=1)
    plt.xlabel('index')
    plt.ylabel('Settle Price')
    plt.legend()
    plt.title('NG1 Price')
    plt.show()

def plot_NG2():
    NG2_table['Settle'].plot(label='Settle Price', linewidth=1)
    plt.xlabel('index')
    plt.ylabel('Settle Price')
    plt.legend()
    plt.title('NG2 Price')
    plt.show()

def plot_NG3():
    NG3_table['Settle'].plot(label='Settle Price', linewidth=1)
    plt.xlabel('index')
    plt.ylabel('Settle Price')
    plt.legend()
    plt.title('NG3 Price')
    plt.show()

def plot_NG4():
    NG4_table['Settle'].plot(label='Settle Price', linewidth=1)
    plt.xlabel('index')
    plt.ylabel('Settle Price')
    plt.legend()
    plt.title('NG4 Price')
    plt.show()

def plot_NG():
    plt.plot(NG1_table['Settle'], label='NG1', linewidth=1)
    plt.plot(NG2_table['Settle'], label='NG2', linewidth=1)
    plt.plot(NG3_table['Settle'], label='NG3', linewidth=1)
    plt.plot(NG4_table['Settle'], label='NG4', linewidth=1)
    plt.xlabel('index')
    plt.ylabel('Settle Price')
    plt.legend()
    plt.show()

def plot_NQ1():
    NQ1_table['Settle'].plot(label='Settle Price', linewidth=1)
    plt.xlabel('index')
    plt.ylabel('Settle Price')
    plt.legend()
    plt.title('NQ1 Price')
    plt.show()

def plot_NQ2():
    NQ2_table['Settle'].plot(label='Settle Price', linewidth=1)
    plt.xlabel('index')
    plt.ylabel('Settle Price')
    plt.legend()
    plt.title('NQ2 Price')
    plt.show()

def plot_NQ():
    plt.plot(NQ1_table['Settle'], label='NQ1', linewidth=1)
    plt.plot(NQ2_table['Settle'], label='NQ2', linewidth=1)
    plt.xlabel('index')
    plt.ylabel('Settle Price')
    plt.legend()
    plt.show()

print('\n\nBonus: Charting Contracts')
plot_dict = {
    'CME_ES1': plot_ES1, 'CME_ES2': plot_ES2,'CME_ES3': plot_ES3,'CME_ES4': plot_ES4, 'CME_ES': plot_ES,
    'CME_CL1': plot_CL1, 'CME_CL2': plot_CL2,'CME_CL3': plot_CL3,'CME_CL4': plot_CL4, 'CME_CL': plot_CL,
    'CME_GC1': plot_GC1, 'CME_GC2': plot_GC2,'CME_GC3': plot_GC3,'CME_GC4': plot_GC4, 'CME_GC': plot_GC,
    'CME_NG1': plot_NG1, 'CME_NG2': plot_NG2,'CME_NG3': plot_NG3,'CME_NG4': plot_NG4, 'CME_NG': plot_NG,
    'CME_NQ1': plot_NQ1, 'CME_NQ2': plot_NQ2,'CME_NQ': plot_NQ}
plot = input('Please type a contract (Ex. CME_ES1) or a contract root (Ex. CME_ES) to see its settle points graphed \nCONTRACT:\n')
plot_dict[plot]()
