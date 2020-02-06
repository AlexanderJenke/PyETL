from crontab import CronTab
import getpass
import os
from optparse import OptionParser


def get_opts_and_args():
    parser = OptionParser()
    parser.add_option("--interval", dest="interval", default="* * * * *",
                      help="configuration of cron following the standard pattern")
    return parser.parse_args()


class ETLCronJob:

    def __init__(self, command):
        self.user = getpass.getuser()
        self.currentdir = os.getcwd()
        self.my_cron = CronTab(self.user)
        self.command = command

    def create_cron_job(self, interval):
        self.interval = interval
        self.job = self.my_cron.new(self.command)
        self.job.setall(self.interval)
        self.job.set_comment("ETL process")
        self.my_cron.write()

    def kill_job(self):
        jobs = self.my_cron.find_comment("ETL process")
        self.my_cron.remove_all(comment="ETL process")
        self.my_cron.write()


if __name__ == "__main__":
    opts, args = get_opts_and_args()
    if len(args != 1):
        print("USAGE: python3 cron.py [MODEL_FILE] [OPTIONS]")
    model_file = args[0]  # Path to model file
    command = f"python3 {os.getcwd()}/main.py {model_file}"
    cronjob = ETLCronJob(command)
    cronjob.kill_job()
    cronjob.create_cron_job(opts.interval)
