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


#ES2 to CSV
# ES1.to_csv('ES1.csv')
# ES2.to_csv('ES2.csv')
# ES3.to_csv('ES3.csv')
# ES4.to_csv('ES4.csv')

#ES2 to pickle
# ES1df = pd.read_csv('ES1.csv')
# ES2df = pd.read_csv('ES2.csv')
# ES3df = pd.read_csv('ES3.csv')
# ES4df = pd.read_csv('ES4.csv')
#
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

# OPEN PICKLE FILE
with open('ES1.pkl', 'rb') as pickle_file:
    ES1_table = pickle.load(pickle_file)

with open('ES2.pkl', 'rb') as pickle_file:
    ES2_table = pickle.load(pickle_file)

with open('ES3.pkl', 'rb') as pickle_file:
    ES3_table = pickle.load(pickle_file)

with open('ES4.pkl', 'rb') as pickle_file:
    ES4_table = pickle.load(pickle_file)

print(ES4_table.head())

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

# print(ES2_table.head())


# TESTING FUNCTION - DOUBLING COLUMN
# ES2_table['Open_double'] = ES2_table['Open']*2
#
# print(ES2_table[['Open','Open_double']])


ES1_table['ret'] = ES1_table['Settle'] / ES1_table['Settle'].shift(1) - 1
ES2_table['ret'] = ES2_table['Settle'] / ES2_table['Settle'].shift(1) - 1
ES3_table['ret'] = ES3_table['Settle'] / ES3_table['Settle'].shift(1) - 1
ES4_table['ret'] = ES4_table['Settle'] / ES4_table['Settle'].shift(1) - 1


# print(ES4_table[['ret']])

ES1_table['volatility'] = ES1_table['ret'].std()
ES2_table['volatility'] = ES2_table['ret'].std()
ES3_table['volatility'] = ES3_table['ret'].std()
ES4_table['volatility'] = ES4_table['ret'].std()

print(ES3_table[['volatility']])



ES4_table['Settle'].plot()
plt.show()
#
# ES_new_data['ann_vol'] = ES_new_data[['volatility']] * math.sqrt(252)
#
# print(ES_new_data[['ann_vol']])
#
# ES_new_data['tr_60_day_vol'] = ES_new_data[['ret']].rolling(60).std()
#
# print(ES_new_data[['tr_60_day_vol']])
#
# ES_new_data['tr_60_day_ann_vol'] = ES_new_data[['ret']].rolling(60).std() * math.sqrt(252)
#
# print(ES_new_data[['tr_60_day_ann_vol']])



# ESdf.to_pickle('ES.pkl')
# ESdf2 = pd.read_pickle('ES.pkl')
# print(ESdf2.head())

# ES1.to_sql(mydb, "app.py", if_exists='replace', flavor='mysql')
