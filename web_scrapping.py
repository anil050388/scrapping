#Import packages
import pandas as pd
import lxml
from pymongo import MongoClient
from dotenv import load_dotenv
import os

#Import the DB password
load_dotenv()
password = os.environ['password']
name = os.environ['user']


#Scrap the list of S&P
url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
payload = pd.read_html(url)
symbol_list = payload[0]

#Save to Local CSV files
symbol_list.to_csv('SAP500_symbol_list.csv')

#Establish a Client Connection
MONGODB_CONNECTION_STRING = "mongodb+srv://"+name+":"+password+"@cluster0.thkfce7.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(MONGODB_CONNECTION_STRING)

# to check if connection has been established
client.server_info()['ok']

# convert to dictionary for uploading to MongoDB
symbol_dict = symbol_list.to_dict('records')

# point to symbolsDB collection 
db = client.symbolsDB

# emtpy symbols collection before inserting new records
db.symbols.drop()

# insert new symbols
db.symbols.insert_many(symbol_dict)



#Push the list to MangoDB