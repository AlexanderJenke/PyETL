from flask import Flask, render_template, request, redirect, url_for, session
from cron import ETLCronJob
from db import ConfigHandler
import os

app = Flask(__name__)
cron_job = ETLCronJob()
handler = ConfigHandler()

@app.route("/")
def index():
    return render_template("login.html")

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
    cron_job.kill_job()
    interval = handler.get_interval()
    cron_job.create_cron_job(interval)
    app.secret_key = os.urandom(12)
    app.run()
