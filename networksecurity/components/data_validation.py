from networksecurity.entity.config_entity import DataValidationConfig
from networksecurity.entity.artifact_entity import DataIngestionArtifact,DataValidationArtifact
from networksecurity.logging.logger import logging
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.contants.training_pipeline import SCHEMA_FILE_PATH
from networksecurity.utils.main_utils.utlis import read_yaml_file,write_yaml_file
from scipy.stats import ks_2samp
import pandas as pd
import os,sys



class DataValidation:
    def __init__(self,data_ingestion_artifact:DataIngestionArtifact,
                 data_validation_config:DataValidationConfig):
        try:
            self.data_ingestion_artifact=data_ingestion_artifact
            self.data_validation_config=data_validation_config
            self.schema_config=read_yaml_file(SCHEMA_FILE_PATH)
            
        except Exception as e:
            raise NetworkSecurityException(e,sys)
    
    @staticmethod
    def read_data(file_path)->pd.DataFrame:
        try:
            return pd.read_csv(file_path)
        
        except Exception as e:
            raise NetworkSecurityException(e,sys)
        
    def validate_number_of_columns(self,dataframe:pd.DataFrame)->bool:
        try:
            number_of_columns=len(self.schema_config)
            logging.info(f"Required number of columns: {number_of_columns}")
            logging.info(f"Data frame has columns:{len(dataframe.columns)}")
            if len(dataframe.columns)==number_of_columns:
                return True
            else:
                return False
        except Exception as e:
            raise NetworkSecurityException(e,sys)
        
    def numerical_column_exist(self,dataframe)-> bool:
        try:
            schema=self.schema_config
            schema_columns = [list(d.keys())[0] for d in schema["columns"]]
            non_int_columns = []

            for col in schema_columns:
                if not pd.api.types.is_numeric_dtype(dataframe[col]):
                    print(non_int_columns)
                    non_int_columns.append(col)

            if len(non_int_columns)==0:
                logging.info("Everything is Numerical column only")
                return False
            else:
                logging.info("there is some non-numerical columns in the dataset")
                logging.info(non_int_columns)
                return True
            
            
        except Exception as e:
            raise NetworkSecurityException(e,sys)
    
    def detect_dataset_drift(self,base_df,current_df,threshold=0.05)->bool:
        try:
            status=True
            report={}
            for column in base_df.columns:
                d1=base_df[column]
                d2=current_df[column]
                is_sample_dist=ks_2samp(d1,d2)
                if threshold<=is_sample_dist.pvalue:
                    is_found=False
                else:
                    is_found=True
                    status=False
                report.update({column:{
                    "p_value":float(is_sample_dist.pvalue),
                    "drift_status":is_found
                }})
            drift_report_file_path=self.data_validation_config.drift_report_file_path
            #create an directory
            dir_path=os.path.dirname(drift_report_file_path)
            os.makedirs(dir_path,exist_ok=True)
            
            write_yaml_file(file_path=drift_report_file_path,content=report)
            
        except Exception as e:
            raise NetworkSecurityException(e,sys)
        
    def initiate_data_validation(self)-> DataValidationArtifact:
        try:
            training_file_path=self.data_ingestion_artifact.trained_file_path
            test_file_path=self.data_ingestion_artifact.test_file_path
            
            #read the data from train and test
            train_dataframe=DataValidation.read_data(training_file_path)
            test_dataframe=DataValidation.read_data(test_file_path)
            
            #validate no of columns
            status=self.validate_number_of_columns(dataframe=train_dataframe)
            if not status:
                error_message=f"Train dataframe does not contain all columns \n"
            status=self.validate_number_of_columns(dataframe=test_dataframe)
            if  status:
                error_message=f" Test dataframe does not contain all columns \n"
            
            logging.info("checking for the numerical only present in the dataframe")   
            status=self.numerical_column_exist(dataframe=train_dataframe)
            if not status:
                error_message=f"Dataframe contains other than interger dtypes"
            status=self.numerical_column_exist(dataframe=train_dataframe)
            if  status:
                error_message=f"Dataframe everything is interger only"
            
            # checking for data-drift
            status=self.detect_dataset_drift(base_df=train_dataframe,current_df=test_dataframe)
            dir_path=os.path.dirname(self.data_validation_config.valid_train_file_path)
            os.makedirs(dir_path,exist_ok=True)
            
            
            train_dataframe.to_csv(
                self.data_validation_config.valid_train_file_path,index=False,header=True
            )
            
            test_dataframe.to_csv(
                self.data_validation_config.valid_test_file_path,index=False,header=True
            )
            
            
            data_validation_artifact= DataValidationArtifact(
                validation_status=status,
                valid_train_file_path=self.data_ingestion_artifact.trained_file_path,
                valid_test_file_path=self.data_ingestion_artifact.test_file_path,
                invalid_train_file_path=None,
                invalid_test_file_path=None,
                drift_report_file_path=self.data_validation_config.drift_report_file_path
            )
            
            return data_validation_artifact
                
        except Exception as e:
            raise NetworkSecurityException(e,sys)
    