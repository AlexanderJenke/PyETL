from crontab import CronTab
import getpass
import os

class ETLCronJob: 

    def __init__(self):
        self.user = getpass.getuser()
        self.currentdir = os.getcwd()
        self.my_cron = CronTab(self.user)
        self.command = "python3 " + self.currentdir + "/etl.py"

    def create_cron_job(self, interval):
        self.interval = interval
        self.job = self.my_cron.new(self.command)
        self.job.setall(self.interval)
        self.my_cron.write()

    def kill_job(self):
        jobs = self.my_cron.find_command(self.command)
        self.my_cron.remove_all(command=self.command)
        self.my_cron.write()
