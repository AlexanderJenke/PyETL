from flask import Flask, render_template, request, redirect, url_for, session, make_response
from cron import ETLCronJob
from db import ConfigHandler
import os
from conn import * 
from fpdf import FPDF

app = Flask(__name__, static_url_path='')
cron_job = ETLCronJob()
handler = ConfigHandler()

@app.route("/")
def index():
    return render_template("login.html")

@app.route("/js/<path:path>")
def send_js(path):
    return send_from_directory("js", path)

@app.route("/css/<path:path>")
def send_css(path):
    return send_from_directory("css", path)

@app.route("/test")
def test():
    return render_template("test.html") 

@app.route("/login", methods= ["POST"])
def login():
    username = request.form["username"]
    password = request.form["password"]
    handler = ConfigHandler()
    correct_username = handler.get_username()
    correct_password = handler.get_password()
    if username == correct_username and password == correct_password:
        session["logged_in"] = True
        return redirect(url_for("config_page"))
    return "DENIED!"

@app.route("/logout", methods = ["GET"])
def logout():
    session["logged_in"] = False
    return redirect(url_for("index"))

@app.route("/config")
def config_page():
    if not session.get("logged_in"):
        return redirect(url_for("index"))
    handler = ConfigHandler()
    return render_template("config.html", host=handler.get_host(), port=handler.get_port(), interval=handler.get_interval(), location = handler.get_csv_dir())

@app.route("/run", methods = ["GET"])
def run_cron_job():
    handler = ConfigHandler()
    cron_job.kill_job()
    interval = handler.get_interval()
    cron_job.create_cron_job(interval)
    return redirect(url_for("config_page"))

@app.route("/patients", methods = ["GET"])
def patient_page():
    data = {}
    data["22"] = ("50", "z", True, ["The patient lies for a too long time in the bed.", "The patient has high blood pressure."])
    data["23"] = ("51", "z", True, ["The patient is quiet old.", "The patient has high blood pressure."])
    data["24"] = ("11", "z", False, [])
    data["25"] = ("2", "z", False, [])
    data["26"] = ("4", "z", False, [])

    data["27"] = ("50", "z", True, ["The patient lies for a too long time in the bed.", "The patient has high blood pressure."])
    data["28"] = ("50", "z", True, ["The patient lies for a too long time in the bed.", "The patient has high blood pressure."])
    data["29"] = ("50", "z", True, ["The patient lies for a too long time in the bed.", "The patient has high blood pressure."])
    data["32"] = ("50", "z", True, ["The patient lies for a too long time in the bed.", "The patient has high blood pressure."])
    data["32"] = ("50", "z", True, ["The patient lies for a too long time in the bed.", "The patient has high blood pressure."])
    data["33"] = ("51", "z", True, ["The patient is quiet old.", "The patient has high blood pressure."])
    data["34"] = ("11", "z", False, [])
    data["45"] = ("2", "z", False, [])
    data["56"] = ("4", "z", False, [])
    data["213"] = ("51", "z", True, ["The patient is quiet old.", "The patient has high blood pressure."])
    data["234"] = ("11", "z", False, [])
    data["252"] = ("2", "z", False, [])
    data["216"] = ("4", "z", False, [])
    data["243"] = ("51", "z", True, ["The patient is quiet old.", "The patient has high blood pressure."])
    data["244"] = ("11", "z", False, [])
    data["235"] = ("2", "z", False, [])
    data["256"] = ("4", "z", False, [])
    data["213"] = ("51", "z", True, ["The patient is quiet old.", "The patient has high blood pressure."])
    data["124"] = ("11", "z", False, [])
    data["125"] = ("2", "z", False, [])
    data["126"] = ("4", "z", False, [])
    data["123"] = ("51", "z", True, ["The patient is quiet old.", "The patient has high blood pressure."])
    data["1224"] = ("11", "z", False, [])
    data["25"] = ("2", "z", False, [])
    data["24126"] = ("4", "z", False, [])
    data["22"] = ("50", "z", True, ["The patient lies for a too long time in the bed.", "The patient has high blood pressure."])
    data["23"] = ("51", "z", True, ["The patient is quiet old.", "The patient has high blood pressure."])
    data["24"] = ("11", "z", False, [])
    data["25"] = ("2", "z", False, [])
    data["26"] = ("4", "z", False, [])
    data["22"] = ("50", "z", True, ["The patient lies for a too long time in the bed.", "The patient has high blood pressure."])
    data["23"] = ("51", "z", True, ["The patient is quiet old.", "The patient has high blood pressure."])
    data["24"] = ("11", "z", False, [])
    data["25"] = ("2", "z", False, [])
    data["26"] = ("4", "z", False, [])
    data["22"] = ("50", "z", True, ["The patient lies for a too long time in the bed.", "The patient has high blood pressure."])
    data["23"] = ("51", "z", True, ["The patient is quiet old.", "The patient has high blood pressure."])
    data["24"] = ("11", "z", False, [])
    data["25"] = ("2", "z", False, [])
    data["26"] = ("4", "z", False, [])
    print(data)
    return render_template("patient.html", data=data)

@app.route("/pdf", methods = ["GET"])
def create_pdf():
    data = {}
    data["24"] = ("11", "z", False, [])
    data["25"] = ("2", "z", False, [])
    data["26"] = ("4", "z", False, [])
    data["22"] = ("50", "z", True, ["The patient lies for a too long time in the bed.", "The patient has high blood pressure. I am a very long text, to check what happens to the pdf"])
    data["23"] = ("51", "z", True, ["The patient is quiet old.", "The patient has high blood pressure."])
    data["24"] = ("11", "z", False, [])
    data["25"] = ("2", "z", False, [])
    data["26"] = ("4", "z", False, [])
    pdf = FPDF()
    pdf.set_font("Arial", size=24)
    pdf.add_page(orientation="L")

    col_width = pdf.w / 11
    row_height = pdf.font_size
    spacing = 1.5
    pdf.cell(col_width, row_height * spacing, txt="Results")
    pdf.ln(row_height*spacing * 2)
    pdf.set_font("Arial", size=10)
    pdf.set_fill_color(238, 238, 238)
    pdf.cell(col_width, row_height * spacing, txt="Patient ID", border=1,fill = True)
    pdf.cell(col_width, row_height * spacing, txt="Patient birthday", border=1, fill=True)
    pdf.cell(col_width * 8, row_height * spacing, txt="Diagnosis", border=1, fill=True)
    pdf.ln(spacing * row_height)
    for key, value in data.items():
        if value[2]:
            pdf.set_fill_color(237, 150, 158)
        new_lines = ""
        for i in range(len(value[3])):
            new_lines += "\n" 
        x = pdf.get_x() + col_width
        y = pdf.get_y()
        pdf.multi_cell(col_width, row_height * spacing, txt=key + new_lines, border=1, fill=True) 
        pdf.set_xy(x, y)
        x = pdf.get_x() + col_width
        y = pdf.get_y()
        pdf.multi_cell(col_width, row_height * spacing, txt=value[0] + new_lines, border=1, fill=True)
        pdf.set_xy(x, y)
        diagnosises = ""
        if value[2]:
            pdf.set_font("Arial", "B", size=10)
            pdf.set_font("Arial", size=10)
            for reason in value[3]:
                if len(reason) > 125:
                    reason = reason[:125]
                diagnosises += reason + "\n"
        pdf.multi_cell(col_width * 8, row_height * spacing, txt=diagnosises, border=1, fill=True)
        pdf.set_fill_color(238, 238, 238)
    response = make_response(pdf.output(dest='S').encode('latin-1'))
    response.headers['Content-Type'] = 'application/pdf'
    response.headers['Content-Disposition'] = \
            'inline; filename=results.pdf'
    return response

@app.route("/cronconfig", methods= ["GET"])
def cron_config():
    new_interval = request.args.get("interval")
    print(request.form)
    print(new_interval)
    handler = ConfigHandler()
    handler.update_interval(new_interval)
    cron_job.kill_job()
    cron_job.create_cron_job(new_interval)
    return redirect(url_for("config_page"))

if __name__ == "__main__":
    #conn = DBReceiver()
    #initer = DBInitializer()
    #conn.create_table()
    #print(conn.receive_reasons())
    #cron_job.kill_job()
    #interval = handler.get_interval()
    #cron_job.create_cron_job(interval)
    app.secret_key = os.urandom(12)
    app.run()
