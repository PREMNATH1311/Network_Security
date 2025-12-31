import os
import sys
import json

from dotenv import load_dotenv

import certifi
import pandas as pd
import numpy as np
import pymongo
from networksecurity.logging.logger import logging
from networksecurity.exception.exception import NetworkSecurityException


load_dotenv()

MONGO_DB_URL=os.getenv("MONGODB_URI")
# print(MONGO_DB_URL)


ca=certifi.where()


class NetworkDataExtrat():
    def __init__(self):
        try:
            pass
        except Exception as e:
            raise NetworkSecurityException(e,sys)
        
    
    def cv_to_json_convertor(self,file_path):
        try:
            data=pd.read_csv(file_path)
            data.reset_index(drop=True,inplace=True)
            # records=list(json.loads(data.T.to_json()).values)
            records = data.to_dict(orient="records")
            return records
            
        except Exception as e:
            raise NetworkSecurityException(e,sys)
        
    def insert_data_mongodb(self,records,database,collection):
        try:
            self.database=database
            self.records=records
            self.collection=collection
            
            self.mongo_client=pymongo.MongoClient(MONGO_DB_URL)
            self.database=self.mongo_client[self.database]
            
            self.collection=self.database[self.collection]
            self.collection.insert_many(self.records)
            
            return (len(self.records))
        
        except Exception as e:
            raise NetworkSecurityException(e,sys)
        
        
if __name__=="__main__":
    FILEPATH=r"Network_Data\phisingData.csv"
    DATABASE="PREMNATHAI"
    collection="Networkdata"
    networkobj=NetworkDataExtrat()
    records=networkobj.cv_to_json_convertor(file_path=FILEPATH)
    print(records)
    no_of_records=networkobj.insert_data_mongodb(records,DATABASE,collection)
    print(no_of_records)
    
