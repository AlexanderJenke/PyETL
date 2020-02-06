from flask import Flask, render_template, request, redirect, url_for, session, make_response
import os
from fpdf import FPDF
from settings import *
from db import DB

app = Flask(__name__, static_url_path='')

def get_dummy_data():
    """ returns dummy data for testing
    """
    data = {}
    data["22"] = ("50", "w", True, ["The patient lies for a too long time in the bed.", "The patient has high blood pressure."])
    data["23"] = ("51", "w", True, ["The patient is quiet old.", "The patient has high blood pressure."])
    data["24"] = ("11", "w",False, [])
    data["25"] = ("2", "w", False, [])
    data["26"] = ("4", "w", False, [])
    data["27"] = ("50", "w", True, ["The patient lies for a too long time in the bed.", "The patient has high blood pressure."])
    data["28"] = ("50", "w", True, ["The patient lies for a too long time in the bed.", "The patient has high blood pressure."])
    data["29"] = ("50", "w", True, ["The patient lies for a too long time in the bed.", "The patient has high blood pressure."])
    data["32"] = ("50", "w", True, ["The patient lies for a too long time in the bed.", "The patient has high blood pressure."])
    data["32"] = ("50", "m", True, ["The patient lies for a too long time in the bed.", "The patient has high blood pressure."])
    data["33"] = ("51", "m", True, ["The patient is quiet old.", "The patient has high blood pressure."])
    data["34"] = ("11", "m", False, [])
    data["45"] = ("2", "m", False, [])
    data["56"] = ("4", "m", False, [])
    data["213"] = ("51", "m", True, ["The patient is quiet old.", "The patient has high blood pressure."])
    data["234"] = ("11", "m", False, [])
    data["252"] = ("2", "m",False, [])
    data["216"] = ("4", "m", False, [])
    data["243"] = ("51", "m", True, ["The patient is quiet old.", "The patient has high blood pressure."])
    data["244"] = ("11", "m",False, [])
    data["235"] = ("2", "d",False, [])
    return data

@app.route("/")
def index():
    """ returns the login page
    """
    return render_template("login.html")

@app.route("/js/<path:path>")
def send_js(path):
    """ allows to serve js files from the static folder
    """
    return send_from_directory("js", path)

@app.route("/css/<path:path>")
def send_css(path):
    """ allows to serve css files from the static folder
    """
    return send_from_directory("css", path)

@app.route("/login", methods= ["POST"])
def login():
    """authenticates the user, if username and password are correct
    """
    username = request.form["username"]
    password = request.form["password"]
    if username == user and password == pw:
        session["logged_in"] = True
        return redirect(url_for("patient_page"))
    return redirect(url_for("index"))

@app.route("/logout", methods = ["GET"])
def logout():
    """ logs the user out
    """
    session["logged_in"] = False
    return redirect(url_for("index"))

@app.route("/patients", methods = ["GET"])
def patient_page():
    """ shows the main page, containing patient data
    """
    if not session.get("logged_in"):
        return redirect(url_for("index"))
    global is_dummy_data
    if not is_dummy_data:
        global db_host
        global db_port
        global db_user
        global db_port
        conn = DB(host=db_host, port=db_port, user=db_user, password=db_pw) 
        data = conn.get_patients_with_reasons()
    else:
        data = get_dummy_data()
    return render_template("patient.html", data=data)

@app.route("/pdf", methods = ["GET"])
def create_pdf():
    """ prints patient data as PDF file
    """
    if not session.get("logged_in"):
        return redirect(url_for("index"))
    pdf = FPDF()
    pdf.set_font("Arial", size=24)
    pdf.add_page(orientation="L")

    col_width = pdf.w / 15
    row_height = pdf.font_size
    spacing = 0.6
    pdf.cell(col_width, row_height * spacing, txt="Results")
    pdf.ln(row_height*spacing * 2)
    pdf.set_font("Arial", size=10)
    pdf.set_fill_color(238, 238, 238)
    pdf.cell(col_width, row_height * spacing, txt="Patient ID", border=1,fill = True)
    pdf.cell(col_width, row_height * spacing, txt="Birthday", border=1, fill=True)
    pdf.cell(col_width, row_height * spacing, txt="Gender", border=1, fill=True)
    pdf.cell(col_width * 11, row_height * spacing, txt="Reason", border=1, fill=True)
    pdf.ln(spacing * row_height)
    global is_dummy_data
    if is_dummy_data:
        data = get_dummy_data()
    else: 
        global db_host
        global db_port
        global db_user
        global db_port
        conn = DB(host=db_host, port=db_port, user=db_user, password=db_pw) 
        data = conn.get_patients_with_reasons()
    j = -1
    for key, value in data.items():
        j += 1
        if value[2]:
            pdf.set_fill_color(237, 150, 158)
        new_lines = ""
        for i in range(len(value[3])):
            new_lines += "\n" 
        x = pdf.get_x() + col_width
        y = pdf.get_y()
        condition = 3 * len(value[3])
        if (j % 3) == 0 and j != 0:
            pdf.add_page(orientation="L")
            x = pdf.get_x() + col_width
            y = pdf.get_y()
        pdf.multi_cell(col_width, row_height * spacing, txt=str(key) + new_lines, border=1, fill=True) 
        pdf.set_xy(x, y)
        x = pdf.get_x() + col_width
        y = pdf.get_y()

        pdf.multi_cell(col_width, row_height * spacing, txt=value[0] + new_lines, border=1, fill=True) 
        pdf.set_xy(x, y)
        x = pdf.get_x() + col_width
        y = pdf.get_y()
        pdf.multi_cell(col_width, row_height * spacing, txt=value[1] + new_lines, border=1, fill=True)
        pdf.set_xy(x, y)
        diagnosises = ""
        
        pdf.set_font("Arial", "B", size=10)
        pdf.set_font("Arial", size=10)
        for reason in value[3]:
            if len(reason) > 125:
                reason = reason[:125]
            diagnosises += reason + "\n"
        
        pdf.multi_cell(col_width * 11, row_height * spacing, txt=diagnosises, border=1, fill=True)
        pdf.set_fill_color(238, 238, 238)
        if value[2]:
            pdf.image("static/warning.png",x - 50, y + 3, 5, 5)
        pdf.ln(0)
    response = make_response(pdf.output(dest='S').encode('latin-1'))
    response.headers['Content-Type'] = 'application/pdf'
    response.headers['Content-Disposition'] = \
            'inline; filename=results.pdf'
    return response

if __name__ == "__main__":
    """ starts a flask server instance
    """
    global is_dummy_data 
    global db_host 
    global db_port 
    global db_user 
    global db_pw 
    global user 
    global pw 
    opts = get_default_opts()
    is_dummy_data = int(opts.dummy_data)
    db_user = opts.db_user
    db_port = opts.db_port
    db_host = opts.db_host
    db_pw = opts.db_pw
    user = opts.user
    pw = opts.pw

    app.secret_key = os.urandom(12)
    app.run()
