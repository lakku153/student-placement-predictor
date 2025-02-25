from flask import Flask,request,render_template
import pickle
from src.pipeline.batch_prediction import customdata,predictpipeline
from src.logging.logger import logging
import pandas as pd
application=Flask(__name__)
app=application

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/predict',methods=['GET','POST'])
def predict_datapoint():
    if request.method=='GET':
        return render_template('home.html')
    else:
        data=customdata(
            cgpa=float(request.form.get('cgpa')),
            internships=int(request.form.get('internships')),
            projects=int(request.form.get('projects')),
            certifications=int(request.form.get('certifications')),
            aptitudetestscore=int(request.form.get('aptitudetestscore')),
            softskillsrating=float(request.form.get('softskillsrating')),
            extracurricularactivities=request.form.get('extracurricularactivities'),
            placementtraining=request.form.get('placementtraining'),
            ssc_marks=int(request.form.get('ssc_marks')),
            hsc_marks=int(request.form.get('hsc_marks'))
        )
        pred_df=data.get_data_as_data_frame()
        print(pred_df)
        logging.info(f"{pred_df.head()}")

        predict_pipe=predictpipeline()
        results=predict_pipe.predict_(pred_df)
        return render_template('home.html',results=round(results[0],2))

if __name__=='__main__':
    app.run(host='0.0.0.0',debug=True)