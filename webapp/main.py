from flask import Flask, render_template, request, redirect, url_for, session, make_response
import os
from fpdf import FPDF
from settings import *

global is_dummy_data 
app = Flask(__name__, static_url_path='')

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

@app.route("/")
def index():
    return render_template("login.html")

@app.route("/js/<path:path>")
def send_js(path):
    return send_from_directory("js", path)

@app.route("/css/<path:path>")
def send_css(path):
    return send_from_directory("css", path)

@app.route("/login", methods= ["POST"])
def login():
    username = request.form["username"]
    password = request.form["password"]
    if username == user and password == pw:
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
    return render_template("config.html", host=db_host, port=db_port)

@app.route("/patients", methods = ["GET"])
def patient_page(): 
    return render_template("patient.html", data=data)

@app.route("/dbconfig", methods = ["GET"])
def dbconfig():
    global db_host
    global db_port
    db_host = request.args.get("host")
    db_port = request.args.get("port")
    return redirect(url_for("config_page"))

@app.route("/pdf", methods = ["GET"])
def create_pdf():
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
        if y > 170:
            pdf.add_page(orientation="L")
            x = pdf.get_x() + col_width
            y = pdf.get_y()
        print(key, x,y)
        pdf.multi_cell(col_width, row_height * spacing, txt=key + new_lines, border=1, fill=True) 
        pdf.set_xy(x, y)
        x = pdf.get_x() + col_width
        y = pdf.get_y()
        print(key, x, y)
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
        pdf.ln(0)
    response = make_response(pdf.output(dest='S').encode('latin-1'))
    response.headers['Content-Type'] = 'application/pdf'
    response.headers['Content-Disposition'] = \
            'inline; filename=results.pdf'
    return response

if __name__ == "__main__":

    global db_host 
    global db_port 
    global db_user 
    global db_pw 
    global user 
    global pw 
    opts = get_default_opts()
    db_user = opts.db_user
    db_port = opts.db_port
    db_host = opts.db_host
    db_pw = opts.db_pw
    user = opts.user
    pw = opts.pw

    app.secret_key = os.urandom(12)
    app.run()
