from flask import Flask, request, render_template
import pickle
from src.pipeline.batch_prediction import customdata, predictpipeline
from src.logging.logger import logging
import pandas as pd

application = Flask(__name__)
app = application

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/start')
def start_predicting():
    return render_template('index.html')

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
        print(pred_df)
        logging.info(f"{pred_df.head()}")

        predict_pipe = predictpipeline()
        results = predict_pipe.predict_(pred_df)

        placement_status = "Congratualations !! You have high chances of getting placed \U0001F44D !!!" if results[0] == 1 else "Sorry !! You have low chances of getting placed \U0001F44E!!"

        # ✅ Generate personalized advice using form data
        user_data = {
            "cgpa": float(request.form.get('cgpa')),
            "internships": int(request.form.get('internships')),
            "projects": int(request.form.get('projects')),
            "certifications": int(request.form.get('certifications')),
            "aptitudetestscore": int(request.form.get('aptitudetestscore')),
            "softskillsrating": float(request.form.get('softskillsrating')),
            "extracurricularactivities": request.form.get('extracurricularactivities'),
            "placementtraining": request.form.get('placementtraining'),
        }

        advice = generate_advice(user_data)

        return render_template('output.html', results=placement_status, advice=advice)


#  Personalized Advice Generator Function
def generate_advice(data):
    advice = []

    if data['cgpa'] < 7.0:
        advice.append("Try to improve your CGPA to at least 7.0 for better chances.")
    else:
        advice.append("Your CGPA is solid. Keep it up!")

    if data['internships'] < 1:
        advice.append("Gaining at least one internship will strengthen your resume.")

    if data['projects'] < 2:
        advice.append("Working on more projects can showcase practical skills.")

    if data['certifications'] < 2:
        advice.append("Pursue additional certifications in your area of interest.")

    if data['aptitudetestscore'] < 60:
        advice.append("Consider practicing more aptitude problems.")

    if data['softskillsrating'] < 3:
        advice.append("Focus on improving communication and soft skills.")

    if data['placementtraining'] == "No":
        advice.append("Consider enrolling in placement training sessions.")

    if data['extracurricularactivities'] == "No":
        advice.append("Engage in extracurriculars to develop team and leadership skills.")

    return advice


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
