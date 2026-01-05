from networksecurity.components.data_ingestion import DataIngestion
from networksecurity.components.data_validation import DataValidation
from networksecurity.components.data_transformation import DataTransformation
from networksecurity.components.model_trainer import ModelTrainer
from networksecurity.entity.config_entity import (DataIngestionConfig,TrainingPipelineConfig,
                                                  DataValidationConfig,DataTransformationConfig,
                                                  ModelTrainerConfig)
from networksecurity.logging.logger import logging
from networksecurity.exception.exception import NetworkSecurityException
import sys


if __name__=="__main__":
    try:
        logging.info("Started data ingestion")
        trainingpipelineconfig=TrainingPipelineConfig()
        data_ingestion_config=DataIngestionConfig(trainingpipelineconfig)
        data_ingestion=DataIngestion(data_ingestion_config)
        logging.info("Initiate the data ingestion")
        data_ingestion_Artifact=data_ingestion.initiate_data_ingestion()
        print(data_ingestion_Artifact)
        logging.info("Stated the data validation process")
        data_validation_config=DataValidationConfig(trainingpipelineconfig)
        data_validation=DataValidation(data_ingestion_Artifact,data_validation_config)
        logging.info("Inititate the data validation")
        data_validation_artifact=data_validation.initiate_data_validation()
        print(data_validation_artifact)
        logging.info("stated the  the data transformation")
        data_transformation_config=DataTransformationConfig(trainingpipelineconfig)
        data_transformation=DataTransformation(data_validation_artifact,data_transformation_config)
        logging.info("initiate the data transformation")
        data_transformation_artifact=data_transformation.initiate_data_transformation()
        print(data_transformation_artifact)
        
        logging.info("Model Trainer Started")
        model_trainer_config=ModelTrainerConfig(trainingpipelineconfig)
        model_trainer=ModelTrainer(model_trainer_config=model_trainer_config,data_transformation_artifact=data_transformation_artifact)
        model_trainer_artifact=model_trainer.initiate_model_trainer()
        logging.info("Model training artifact created")
        print(model_trainer_artifact)
    except Exception as e:
        raise NetworkSecurityException(e,sys)
    
