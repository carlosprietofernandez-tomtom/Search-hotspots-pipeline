from apscheduler.schedulers.blocking import BlockingScheduler
import datetime
import os
import subprocess


def scheduled_job():
    # call the python script to execute
    subprocess.call(["python", "pipeline/local_pipeline.py"])
    print(f"Running cron monthly script at {datetime.datetime.now()}")


sched = BlockingScheduler()


sched.add_job(scheduled_job, "cron", day="3", hour="1", minute="30")


sched.start()
