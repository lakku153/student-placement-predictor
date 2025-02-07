from src.exception.exception import Exceptionhandle
from src.logging.logger import logging

from src.entity.config_entity import DataIngestionConfig
from src.entity.artifact_entity import DataIngestionArtifact

import os
import sys
import numpy as np
import pandas as pd 
import pypyodbc
from typing import List
from sklearn.model_selection import train_test_split
from dotenv import load_dotenv
load_dotenv()
from push_data import DatabaseManager

class DataIngestion:
    def __init__(self,data_ingestion_config:DataIngestionConfig):
        try:
            self.data_ingestion_config=data_ingestion_config
        except Exception as e:
            raise Exceptionhandle(e,sys)
        
    def export_collection_as_dataframe(self):
        server = os.getenv("server")
        database = os.getenv("database")
        # self.connection_string = f"""DRIVER={{SQL SERVER}};
        #                             SERVER={server};
        #                             DATABASE={database};
        #                             Trust_Connection=yes;
        #                         """
        try:
            # conn=pypyodbc.connect(self.connection_string)
            db_obj=DatabaseManager(server,database)
            conn=db_obj.connect()
            query = f"SELECT * FROM {self.data_ingestion_config.table_name}"

             # Load the query results into a DataFrame
            df = pd.read_sql(query, conn)
            logging.info("dataframe loaded successfully from database")
            return df
        except Exception as e:
            raise Exceptionhandle(e,sys)
    
    def export_data_into_feature_store(self,dataframe:pd.DataFrame):
        try:
            feature_store_file_path=self.data_ingestion_config.feature_store_file_path
            dir_path=os.path.dirname(feature_store_file_path)
            os.makedirs(dir_path,exist_ok=True)
            dataframe.to_csv(feature_store_file_path,index=False,header=True)
            return dataframe
        except Exception as e:
            raise Exceptionhandle(e,sys)
    
    def split_Data_as_train_test(self,dataframe:pd.DataFrame):
        try:
            train_set,test_set=train_test_split(dataframe,test_size=self.data_ingestion_config.train_test_split_ratio)
            logging.info("Performed train test split on the data")
            logging.info("Exited split_data_as_train_test method of Data_Ingeston class")
            dir_path=os.path.dirname(self.data_ingestion_config.training_file_path)
            os.makedirs(dir_path,exist_ok=True)
            logging.info(f"Exporting train and test file path")
            train_set.to_csv(self.data_ingestion_config.training_file_path,index=False,header=True)
            test_set.to_csv(self.data_ingestion_config.testing_file_path,index=False,header=True)
            logging.info(f"Exported train and test file path")
        except Exception as e:
            raise Exceptionhandle(e,sys)
        
    def initiate_data_ingestion(self):
        try:
            dataframe=self.export_collection_as_dataframe()
            dataframe=self.export_data_into_feature_store(dataframe)
            self.split_Data_as_train_test(dataframe)
            dataingestionartifact=DataIngestionArtifact(trained_file_path=self.data_ingestion_config.training_file_path,test_file_path=self.data_ingestion_config.testing_file_path)
            return dataingestionartifact
        except Exception as e:
            raise Exceptionhandle(e,sys)


if __name__=="__main__":
    obj=DataIngestion(DataIngestionConfig)
    obj.initiate_data_ingestion()
    logging.info("Initiate data ingestion ran successfully")