from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging
from networksecurity.contants import training_pipeline
from networksecurity.entity.config_entity import DataIngestionConfig
from networksecurity.entity.artifact_entity import DataIngestionArtifact
import os
import sys
import pymongo
from sklearn.model_selection import train_test_split
from dotenv import load_dotenv
import pandas as pd
import numpy as np
load_dotenv()

MONGODB_URL=os.getenv("MONGODB_URI")

class DataIngestion:
    def __init__(self,data_ingestion_config:DataIngestionConfig):
        try:
            self.data_ingestion_config=data_ingestion_config
        except Exception as e:
            raise NetworkSecurityException(e,sys)
    def export_collection_as_dataframe(self):
        try:
            database_name = self.data_ingestion_config.database_name
            collection_name = self.data_ingestion_config.collection_name

            self.mongo_client = pymongo.MongoClient(MONGODB_URL)
            collection = self.mongo_client[database_name][collection_name]

            data = list(collection.find())
            logging.info(f"Number of records fetched from MongoDB: {len(data)}")

            if len(data) == 0:
                raise ValueError("No records found in MongoDB collection")

            df = pd.DataFrame(data)

            if "_id" in df.columns:
                df.drop(columns=["_id"], axis=1, inplace=True)

            df.replace({"na": np.nan}, inplace=True)

            logging.info(f"DataFrame shape after loading: {df.shape}")

            return df

        except Exception as e:
            raise NetworkSecurityException(e, sys)

        
    def export_data_into_feature_store(self,dataframe:pd.DataFrame):
        try:
            feature_store_file_path=self.data_ingestion_config.feature_store_file_path
            dir_path=os.path.dirname(feature_store_file_path)
            os.makedirs(dir_path,exist_ok=True)
            dataframe.to_csv(feature_store_file_path,index=False,header=True)
            return dataframe
        except Exception as e:
            raise NetworkSecurityException(e,sys)
    def split_data_as_train_test(self, dataframe: pd.DataFrame):
        try:
            if dataframe is None or dataframe.empty:
                raise ValueError("DataFrame is empty. Cannot perform train-test split.")

            train_set, test_set = train_test_split(
                dataframe,
                test_size=self.data_ingestion_config.train_test_split_ratio,
                random_state=42
            )

            logging.info("Performed train test split on the dataframe")

            dir_path = os.path.dirname(self.data_ingestion_config.training_file_path)
            os.makedirs(dir_path, exist_ok=True)

            train_set.to_csv(self.data_ingestion_config.training_file_path, index=False, header=True)
            test_set.to_csv(self.data_ingestion_config.testing_file_path, index=False, header=True)

        except Exception as e:
            raise NetworkSecurityException(e, sys)
            
            
    def initiate_data_ingestion(self):
        try:
            dataframe=self.export_collection_as_dataframe()
            dataframe=self.export_data_into_feature_store(dataframe)
            self.split_data_as_train_test(dataframe)
            
            data_ingestion_artifact=DataIngestionArtifact(trained_file_path=self.data_ingestion_config.training_file_path,
                                                          test_file_path=self.data_ingestion_config.testing_file_path)
            
            return data_ingestion_artifact
        except Exception as e:
            raise NetworkSecurityException(e,sys)