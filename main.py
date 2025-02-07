from src.exception.exception import Exceptionhandle
from src.logging.logger import logging

from src.entity.config_entity import DataIngestionConfig
from src.entity.artifact_entity import DataIngestionArtifact
from src.components.data_ingestion import DataIngestion
from src.entity.config_entity import TrainingPipelineConfig

import sys


if __name__=="__main__":
    try:  
        training_pipeline_config=TrainingPipelineConfig()
        data_ingestion_config=DataIngestionConfig(training_pipeline_config)
        data_ingestion=DataIngestion(data_ingestion_config)
        logging.info("Initiate the data Ingestion")
        dataingestionartifact=data_ingestion.initiate_data_ingestion()
        print(dataingestionartifact)
    except Exception as e:
        raise Exceptionhandle(e,sys)