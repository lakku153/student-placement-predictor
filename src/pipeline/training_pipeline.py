import os
import sys

from src.exception.exception import Exceptionhandle
from src.logging.logger import logging

from src.components.data_ingestion import DataIngestion
from src.components.data_validation import DataValidation
from src.components.data_transformation import DataTransformation
from src.components.model_trainer import ModelTrainer

from src.entity.config_entity import(
    TrainingPipelineConfig,
    DataIngestionConfig,
    DataValidationConfig,
    DataTransformationConfig,
    ModelTrainerConfig,
)

from src.entity.artifact_entity import (
    DataIngestionArtifact,
    DataValidationArtifact,
    DataTransformationArtifact,
    ModelTrainerArtifact,
)

class TrainingPipeline:
    def __init__(self):
        self.trainig_pipeline_config=TrainingPipelineConfig()
        def start_data_ingestion(self):
            try:
                self.data_ingestion_config=DataIngestionConfig(training_pipeline_config=self.trainig_pipeline_config)
                logging.info("Start data ingestion")
                data_ingestion=DataIngestion(data_ingestion_config=self.data_ingestion.config)
                data_ingestion_artifact=data_ingestion.initiate_data_ingestion()
                logging.info(f"Data ingestion completed and artifact: {data_ingestion_artifact}")
                return data_ingestion_artifact

            except Exception as e:
                raise Exceptionhandle(e,sys)
        def start_data_validation(self,data_ingestion_artifact:DataIngestionArtifact):
            try:
                self.data_validation_config=DataValidationConfig(training_pipeline_config=self.trainig_pipeline_config)
                logging.info("Start data validation")
                data_validation=DataValidation(data_ingestion_artifact=data_ingestion_artifact,data_validation_config=self.data_validation.config)
                data_validation_artifact=data_validation.initiate_data_validation()
                logging.info(f"Data validation completed and artifact: {data_validation_artifact}")
                return data_validation_artifact

            except Exception as e:
                raise Exceptionhandle(e,sys)
        
        def start_data_transformation(self,data_validation_artifact:DataValidationArtifact):
            try:
                self.data_transformation_config=DataValidationConfig(training_pipeline_config=self.trainig_pipeline_config)
                logging.info("Start data transformation")
                data_transformation=DataTransformation(data_validation_artifact=data_validation_artifact,data_transformation_config=self.data_transformation_config)
                data_transformation_artifact=data_transformation.initiate_data_transformation()
                logging.info(f"Data transformation completed and artifact: {data_transformation_artifact}")
                return data_transformation_artifact

            except Exception as e:
                raise Exceptionhandle(e,sys)

        def start_model_trainer(self,data_transformation_artifact:DataTransformationArtifact)->ModelTrainerArtifact:
            try:
                self.model_trainer_config=ModelTrainerConfig(
                    trainig_pipeline_config=self.trainig_pipeline_config
                )
                logging.info("started the model traininig")
                model_trainer=ModelTrainer(
                    data_transformation_artifact=data_transformation_artifact,
                    model_trainer_config=self.model_trainer_config
                )
                model_trainer_artifact=model_trainer.initiate_model_trainer()
                logging.info(f"model trainingcompleted and artifact: {model_trainer_artifact}")
                return model_trainer_artifact
            except Exception as e:
                return Exceptionhandle(e,sys)

        def run_pipeline(self):
            try:
                data_ingestion_artifact=self.start_data_ingestion()
                data_validation_artifact=self.start_data_validation(data_ingestion_artifact=data_ingestion_artifact)
                data_transformation_artifact=self.start_data_transformation(data_validation_artifact=data_validation_artifact)
                model_trainer_artifact=self.start_model_trainer(data_transformation_artifact=data_transformation_artifact)
                return model_trainer_artifact
            except Exception as e:
                raise Exceptionhandle(e,sys)