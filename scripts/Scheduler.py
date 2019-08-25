from pytz import utc
import os
import json
import datetime
import traceback

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

        stackTrace = None

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
                if(self.checkRun() == True):
                        try:
                                FetchRentalsDue.main()
                        except Exception:    
                                self.stackTrace = traceback.format_exc()
                                if(self.stackTrace == None):
                                        self.stackTrace = "No error"
                                self.errorHandler("Fetch rentals", self.stackTrace)

        def generatePDF(self):
                if(self.checkRun() == True):
                        try:
                                GenerateRentalPDF.main()
                        except Exception:
                                self.stackTrace = traceback.format_exc()
                                if(self.stackTrace == None):
                                        self.stackTrace = "No error"
                                self.errorHandler("GenerateRentalPDF", self.stackTrace)

        def email(self):
                if(self.checkRun() == True):
                        try:
                                Complete.main()
                        except Exception:
                                self.stackTrace = traceback.format_exc()
                                if(self.stackTrace == None):
                                        self.stackTrace = "No error"
                                self.errorHandler("Complete", self.stackTrace)
                        
        def errorHandler(self, process, error):
                errorInfo = []
                dateToday = datetime.datetime.now()
                errorPath = "../files/errors/" + dateToday.strftime("%Y") + "/" + dateToday.strftime("%m") + "/" + dateToday.strftime("%d")
                directoryName = os.path.dirname(__file__)
                try:
                        os.makedirs(os.path.join(directoryName, errorPath))
                except FileExistsError:
                        pass
                
                try:
                        with open(os.path.join(directoryName, errorPath + "/errors.json"), 'r') as error_file:
                                errorInfo = json.load(error_file)
                except:
                        pass
                
                errorInfo.append({ "process": process, "errorMessage": error})
                with open(os.path.join(directoryName, errorPath + "/errors.json"), 'w') as json_file:
                        json.dump(errorInfo, json_file)