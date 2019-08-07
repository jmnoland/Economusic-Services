from pytz import utc
import os

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor

import FetchRentalsDue
import GenerateRentalPDF
import Complete

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

import logging


class Scheduler():

        path = os.path.join(os.path.dirname(__file__), '../environment/credentials.json')
        cred = credentials.Certificate(path)
        app = firebase_admin.initialize_app(cred, name="SchedulerApp")
        db = firestore.client(app=app)

        def __init__(self):
                executors = {
                        'default': ThreadPoolExecutor(10),
                        'processpool': ProcessPoolExecutor(5)
                }
                job_defaults = {
                        'coalesce': True,
                        'max_instances': 3
                }
                scheduler = BlockingScheduler(executors=executors, job_defaults=job_defaults, timezone=utc)
                
                log = logging.getLogger('apscheduler.executors.default')
                log.setLevel(logging.INFO) 
                fmt = logging.Formatter('%(levelname)s:%(name)s:%(message)s')
                h = logging.StreamHandler()
                h.setFormatter(fmt)
                log.addHandler(h)
                
                job3 = scheduler.add_job(self.email, 'cron', hour='8')
                job1 = scheduler.add_job(self.fetchRentals, 'cron', hour='12')
                job2 = scheduler.add_job(self.generatePDF, 'cron', hour='14')

                scheduler.start()

        def checkRun(self):
                check_ref = self.db.collection(u'check').document(u'0')
                value = check_ref.get()
                return value.to_dict()['val']

        def fetchRentals(self):
                print("Get Rentals Due")
                if(self.checkRun() == True):
                        FetchRentalsDue.main()

        def generatePDF(self):
                print("Generate PDF")
                if(self.checkRun() == True):
                        GenerateRentalPDF.main()

        def email(self):
                print("Email")
                if(self.checkRun() == True):
                        Complete.main()
                        