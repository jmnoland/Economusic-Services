from pytz import utc
import os
import json
import datetime
import traceback
import sqlite3
from dateutil.relativedelta import relativedelta

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
        conn = sqlite3.connect(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Job.db'))
        __cur = conn.cursor()

        stackTrace = None

        def __init__(self):
                try:
                        self.__cur.execute("""CREATE TABLE IF NOT EXISTS Jobs (
                                        Process TEXT NOT NULL,
                                        Complete BIT NOT NULL,
                                        Runtime TIMESTAMP NOT NULL
                                );""")
                        self.conn.commit()
                except Exception:
                        self.stackTrace = traceback.format_exc()
                        if(self.stackTrace == None):
                                self.stackTrace = "No error"
                        self.errorHandler("SQLiteConnection", self.stackTrace)

                executors = {
                        'default': ThreadPoolExecutor(10),
                        'processpool': ProcessPoolExecutor(5)
                }
                job_defaults = {
                        'coalesce': True,
                        'max_instances': 3
                }
                scheduler = BlockingScheduler(executors=executors, job_defaults=job_defaults, timezone=utc)
                
                self.createJobs(scheduler)
                scheduler.start()
                
        def readJobs(self):
                self.__cur.execute('''SELECT * FROM Jobs ORDER BY Runtime''')
                for row in self.__cur:
                        print(row)

        def delete(self):
                self.__cur.execute("DELETE FROM Jobs")
                self.conn.commit()

        def createJobs(self, scheduler):
                now = datetime.datetime.today()
                self.__cur.execute('''SELECT * FROM Jobs 
                                WHERE Complete = 0 AND Runtime < ? 
                                ORDER BY Runtime''', (now,))
                pastJobs = [row for row in self.__cur]

                for job in pastJobs:
                        print(job)
                        if job[0] == 'FetchRentals':
                                self.fetchRentals()
                        if job[0] == 'GenerateRentalPDF':
                                self.generatePDF()
                        if job[0] == 'Complete':
                                self.email()
                
                scheduler.add_job(self.email, 'cron', hour='8')
                scheduler.add_job(self.generatePDF, 'cron', hour='14')
                scheduler.add_job(self.fetchRentals, 'cron', hour='12')
                scheduler.add_job(self.dbMaintenance, 'cron', day_of_week=6)
               
        def checkRun(self):
                check_ref = self.db.collection(u'check').document(u'0')
                value = check_ref.get()
                return value.to_dict()['val']

        def fetchRentals(self):
                if(self.checkRun() == True):
                        try:
                                FetchRentalsDue.main()
                                self.__cur.execute(''' UPDATE Jobs SET Complete = 1 WHERE Complete = 0 AND Process LIKE 'FetchRentals' ''')
                                self.__cur.execute("INSERT INTO Jobs VALUES (?,?,?)", ("FetchRentals", False, self.newDateTime(datetime.datetime.today(), 12)))
                                self.conn.commit()
                        except Exception:
                                self.stackTrace = traceback.format_exc()
                                if(self.stackTrace == None):
                                        self.stackTrace = "No error"
                                self.errorHandler("Fetch rentals", self.stackTrace)

        def generatePDF(self):
                if(self.checkRun() == True):
                        try:
                                GenerateRentalPDF.main()
                                self.__cur.execute(''' UPDATE Jobs SET Complete = 1 WHERE Complete = 0 AND Process LIKE 'GenerateRentalPDF' ''')
                                self.__cur.execute("INSERT INTO Jobs VALUES (?,?,?)", ("GenerateRentalPDF", False, self.newDateTime(datetime.datetime.today(), 14)))
                                self.conn.commit()
                        except Exception:
                                self.stackTrace = traceback.format_exc()
                                if(self.stackTrace == None):
                                        self.stackTrace = "No error"
                                self.errorHandler("GenerateRentalPDF", self.stackTrace)

        def email(self):
                if(self.checkRun() == True):
                        try:
                                Complete.main()
                                self.__cur.execute(''' UPDATE Jobs SET Complete = 1 WHERE Complete = 0 AND Process LIKE 'Complete' ''')
                                self.__cur.execute("INSERT INTO Jobs VALUES (?,?,?)", ("Complete", False, self.newDateTime(datetime.datetime.today(), 8)))
                                self.conn.commit()
                        except Exception:
                                self.stackTrace = traceback.format_exc()
                                if(self.stackTrace == None):
                                        self.stackTrace = "No error"
                                self.errorHandler("Complete", self.stackTrace)
        
        def dbMaintenance(self):
                clearDate = datetime.datetime.today()
                clearDate = clearDate.replace(month=(clearDate.month - 2))
                self.__cur.execute(''' DELETE FROM Jobs WHERE Runtime < ? ''', (clearDate,))
                self.conn.commit()

        def newDateTime(self, dtObj, nHour, addDay=1):
                newDate = dtObj + datetime.timedelta(days=addDay)
                return newDate.replace(hour=nHour, minute=0, second=0, microsecond=0)
                        
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
Scheduler()