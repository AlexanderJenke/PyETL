from crontab import CronTab
from settings import get_default_opts
import getpass
import os

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
    opts = get_default_opts()
    print(opts)
    command = f"python3 {os.getcwd()}/main.py {opts.csv_dir} --db_host={opts.db_host} --db_port={opts.db_port}"
    cronjob = ETLCronJob(command)
    cronjob.kill_job()
    cronjob.create_cron_job(opts.interval)

