import sys
import pandas as pd
from src.exception.exception import Exceptionhandle

from src.utils.main_utils.utils import load_object


class predictpipeline:
    def __init__(self):
        pass
    
    def predict_(self,features):
        try:
            model_path='final_model/model.pkl'
            preprocessor_path='final_model/preprocessor.pkl'
            model=load_object(file_path=model_path)
            preprocessor=load_object(file_path=preprocessor_path)
            data_scaled=preprocessor.transform(features)
            preds=model.predict(data_scaled)
            return preds
        except Exception as e:
            raise Exceptionhandle(e,sys)

class customdata():
    def __init__(self,age,sex,bmi,children,smoker,region):
        self.age=age
        self.sex=sex
        self.bmi=bmi
        self.children=children
        self.smoker=smoker
        self.region=region

    def get_data_as_data_frame(self):
        try:
            custom_data_input_dict = {
                "age": [self.age],
                "sex": [self.sex],
                "bmi": [self.bmi],
                "children": [self.children],
                "smoker": [self.smoker],
                "region": [self.region]
            }

            return pd.DataFrame(custom_data_input_dict)

        except Exception as e:
            raise Exceptionhandle(e, sys)