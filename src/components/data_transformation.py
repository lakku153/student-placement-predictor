import sys,os
import numpy as np
import pandas as pd
# from sklearn.impute import KNNImputer
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import LabelEncoder

from src.constant.training_pipeline import TARGET_COLUMN

from src.entity.artifact_entity import (DataTransformationArtifact,DataValidationArtifact)
from src.entity.config_entity import DataTransformationConfig

from src.exception.exception import Exceptionhandle
from src.logging.logger import logging

from src.utils.main_utils.utils import save_numpy_array_data,save_object

from sklearn.base import BaseEstimator, TransformerMixin

class BinaryEncoder(BaseEstimator, TransformerMixin):
    """
    Custom transformer to encode binary categorical columns ('Yes'/'No') as 1/0.
    """
    def __init__(self, binary_columns=None):
        self.binary_columns = binary_columns  # List of binary columns to encode

    def fit(self, X, y=None):
        return self  # No fitting required

    def transform(self, X):
        X = X.copy()  # Avoid modifying original DataFrame
        if self.binary_columns:
            for col in self.binary_columns:
                if col in X.columns:
                    X[col] = X[col].map({'Yes': 1, 'No': 0})  # Convert Yes -> 1, No -> 0
        return X
    def get_feature_names_out(self, input_features=None):
        """
        Ensures compatibility with ColumnTransformer.
        """
        return np.array(self.binary_columns) if self.binary_columns else np.array([])


class DataTransformation:
    def __init__(self,data_validation_artifact:DataValidationArtifact,data_transformation_config:DataTransformationConfig):
        try:
            self.data_validation_artifact:DataValidationArtifact=data_validation_artifact
            self.data_transformation_config:DataValidationArtifact=data_transformation_config
        except Exception as e:
            raise Exceptionhandle(e,sys)

    @staticmethod
    def read_data(file_path):
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise Exceptionhandle(e,sys)

    def get_data_transformer_object(cls)->Pipeline:
         """

        Returns:
          A Pipeline object
        """
         logging.info("Entered get_data_transformer_object method of transformation class")
         try:
            numeric_columns = ['aptitudetestscore','ssc_marks', 'hsc_marks']
            cat_columns=['extracurricularactivities','placementtraining']

            binary_encoder = BinaryEncoder(binary_columns=cat_columns)
            preprocessor=ColumnTransformer([
                ("num",StandardScaler(),numeric_columns),
                ("cat",binary_encoder,cat_columns)
            ],
             remainder='passthrough')
            
            logging.info("Column transformer implementd for the preprocessing pipeline.")
            return preprocessor
         except Exception as e:
             raise Exceptionhandle(e,sys)


    def initiate_data_transformation(self)->DataTransformationArtifact:
        logging.info("Entered initiate data transformation method")
        try:
            logging.info("Starting data transformation")
            train_df=DataTransformation.read_data(self.data_validation_artifact.valid_train_file_path)
            test_df=DataTransformation.read_data(self.data_validation_artifact.valid_test_file_path)

            train_df.drop(columns=['studentid'],inplace=True)
            test_df.drop(columns=['studentid'],inplace=True)
            # print(train_df.loc[1])

            # training dataframe
            input_feature_train_df=train_df.drop(columns=[TARGET_COLUMN],axis=1)
            target_feature_train_df=train_df[TARGET_COLUMN]

            # testing dataframe
            input_feature_test_df=test_df.drop(columns=[TARGET_COLUMN],axis=1)
            target_feature_test_df=test_df[TARGET_COLUMN]

            label_encoder = LabelEncoder()
            logging.info("Initialized label encoder")
            target_feature_train_df=label_encoder.fit_transform(target_feature_train_df.values.ravel())
            target_feature_test_df=label_encoder.transform(target_feature_test_df.values.ravel())
            

            logging.info("Target columns converted into integer values")
            original_cols=list(input_feature_train_df.columns)
            numeric_columns = ['aptitudetestscore','ssc_marks', 'hsc_marks']
            cat_columns=['extracurricularactivities','placementtraining']
            remain_cols=[col for col in original_cols if col not in numeric_columns+cat_columns]
            all_feature_names = [*numeric_columns,*cat_columns,*remain_cols]
            logging.info(f"All feature names {all_feature_names}")

            preprocessor=self.get_data_transformer_object()
            
            # Apply transformation
            transformed_train = preprocessor.fit_transform(input_feature_train_df)
            transformed_test = preprocessor.transform(input_feature_test_df)
            
            # Convert to DataFrame with correct column names
            transformed_train_df = pd.DataFrame(transformed_train, columns=all_feature_names)
            transformed_test_df = pd.DataFrame(transformed_test, columns=all_feature_names)
            
            
            transformed_train_df['placementstatus']=target_feature_train_df
            transformed_test_df['placementstatus']=target_feature_test_df
            
            logging.info("converting the datatype of some columns")
            transformed_train_df=transformed_train_df.astype({'aptitudetestscore':'float64',  'ssc_marks':'float64',  'hsc_marks':'float64',  'extracurricularactivities':'int32',  'placementtraining':'int32',  'cgpa':'float32',  'internships':'int32',  'projects':'int32',  'certifications':'int32',  'softskillsrating':'float32',  'placementstatus':'int32'})
            transformed_test_df=transformed_test_df.astype({'aptitudetestscore':'float64',  'ssc_marks':'float64',  'hsc_marks':'float64',  'extracurricularactivities':'int32',  'placementtraining':'int32',  'cgpa':'float32',  'internships':'int32',  'projects':'int32',  'certifications':'int32',  'softskillsrating':'float32',  'placementstatus':'int32'})
            logging.info("Converted the data type of columns")
            print(transformed_test_df.head(1))
                      
            logging.info("Saving train array and test array after the preprocessing pipeline")

            dir_path=os.path.dirname(self.data_transformation_config.transformed_train_file_path)
            os.makedirs(dir_path,exist_ok=True)
            dir_path=os.path.dirname(self.data_transformation_config.transformed_train_file_path)
            os.makedirs(dir_path,exist_ok=True)
            transformed_train_df.to_csv(self.data_transformation_config.transformed_train_file_path,index=False,header=True, float_format="%.3f")
            transformed_test_df.to_csv(self.data_transformation_config.transformed_test_file_path,index=False,header=True,float_format="%.3f")
            
            logging.info("Saved train array and test array after the preprocessing pipeline")
            save_object(self.data_transformation_config.transformed_object_file_path,preprocessor)

            save_object("final_model/preprocessor.pkl",preprocessor)

            #preparing artifacts
            data_transformation_artifact=DataTransformationArtifact(
                transformed_object_file_path=self.data_transformation_config.transformed_object_file_path,
                transformed_train_file_path=self.data_transformation_config.transformed_train_file_path,
                transformed_test_file_path=self.data_transformation_config.transformed_test_file_path,
            )
            return data_transformation_artifact

        except Exception as e:
            raise Exceptionhandle(e,sys)