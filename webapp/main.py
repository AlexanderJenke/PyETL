from flask import Flask, render_template, request, redirect, url_for
from cron import ETLCronJob
from db import ConfigHandler

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
    if username == "markus" and password == "1":
        return redirect(url_for("config_page"))
    return "DENIED!"

@app.route("/config")
def config_page():
    return render_template("config.html")

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
    print(interval)
    cron_job.create_cron_job(interval)
    app.run()
