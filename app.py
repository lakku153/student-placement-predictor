from flask import Flask, request, render_template
import pickle
from src.pipeline.batch_prediction import customdata, predictpipeline
from src.logging.logger import logging
import pandas as pd
import re

application = Flask(__name__)
app = application

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/start')
def start_predicting():
    return render_template('index.html')
####


@app.route('/predict', methods=['GET', 'POST'])
def predict_datapoint():
    if request.method == 'GET':
        return render_template('home.html')
    else:
        data = customdata(
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

        pred_df = data.get_data_as_data_frame()
        logging.info(f"{pred_df.head()}")

        predict_pipe = predictpipeline()
        results = predict_pipe.predict_(pred_df)

        placement_status = "Congratualations !! You have high chances of getting placed \U0001F44D !!!" if results[0] == 1 else "Sorry !! You have low chances of getting placed \U0001F44E!!"

        # ✅ Advice must be generated here
        advice_list = generate_advice(pred_df.iloc[0].to_dict())

        return render_template('output.html', results=placement_status, advice=advice_list)


#  Personalized Advice Generator Function
def generate_advice(data):
    advice = []

    if data['cgpa'] < 7.0:
        advice.append(("Try to improve your CGPA to at least 7.0.", None))
    else:
        advice.append(("Your CGPA is solid. Keep it up!", None))

    if data['internships'] < 1:
        advice.append(("Gain at least one internship to strengthen your resume.", None))

    if data['projects'] < 2:
        advice.append(("Work on more projects to showcase skills.", None))

    if data['certifications'] < 2:
        advice.append(("Pursue additional certifications in your field.", None))

    if data['aptitudetestscore'] < 60:
        advice.append(("Practice more aptitude problems.", "apti"))

    if data['softskillsrating'] < 3:
        advice.append(("Improve your soft skills through communication exercises.", "softskills"))

    if data['placementtraining'] == "No":
        advice.append(("Enroll in placement training sessions.", "training"))

    if data['extracurricularactivities'] == "No":
        advice.append(("Engage in extracurricular activities to build leadership.", "extracurricular"))

    return advice


@app.route('/about')
def about():
    return render_template('about.html')



@app.route('/advice/<topic>')
def advice_page(topic):
    return render_template(f"{topic}.html")


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
