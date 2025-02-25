import sys
import pandas as pd
from src.exception.exception import Exceptionhandle
from src.logging.logger import logging
from src.utils.main_utils.utils import load_object
from src.utils.ml_utils.model.estimator import PlacedModel

class predictpipeline:
    def __init__(self):
        pass
    
    def predict_(self,features):
        try:
            logging.info(f"in batch prediction pipeline {features}")
            model_path='final_model/model.pkl'
            preprocessor_path='final_model/preprocessor.pkl'
            model=load_object(file_path=model_path)
            preprocessor=load_object(file_path=preprocessor_path)
            pred_obj=PlacedModel(preprocessor,model)
            preds=pred_obj.predict(features)
            return preds
        except Exception as e:
            raise Exceptionhandle(e,sys)

class customdata():
    def __init__(self,cgpa,internships,projects,certifications,aptitudetestscore,softskillsrating,extracurricularactivities,placementtraining,ssc_marks,hsc_marks):
        self.cgpa=cgpa
        self.internships=internships
        self.projects=projects
        self.certifications=certifications
        self.aptitudetestscore=aptitudetestscore
        self.softskillsrating=softskillsrating
        self.extracurricularactivities=extracurricularactivities
        self.placementtraining=placementtraining
        self.ssc_marks=ssc_marks
        self.hsc_marks=hsc_marks

    def get_data_as_data_frame(self):
        try:
            custom_data_input_dict = {
                "cgpa": [self.cgpa],
                "internships": [self.internships],
                "projects": [self.projects],
                "certifications": [self.certifications],
                "aptitudetestscore": [self.aptitudetestscore],
                "softskillsrating": [self.softskillsrating],
                "extracurricularactivities": [self.extracurricularactivities],
                "placementtraining": [self.placementtraining],
                "ssc_marks": [self.ssc_marks],
                "hsc_marks": [self.hsc_marks]
            }

            return pd.DataFrame(custom_data_input_dict)

        except Exception as e:
            raise Exceptionhandle(e, sys)