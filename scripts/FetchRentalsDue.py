import os
import json
import datetime
import threading

try:
    import queue
except ImportError:
    import Queue as queue

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

rentalResults = []
clientResults = []
rentalsComplete = threading.Event()

dirname = os.path.dirname(__file__)
filename = os.path.join(dirname, '../environment/credentials.json')

cred = credentials.Certificate(filename)
app = firebase_admin.initialize_app(cred, name="QueryApp")
db = firestore.client(app=app)

q = queue.Queue()

def main():
    date = datetime.datetime.today()
    utcOffset = datetime.timedelta(hours=2, seconds=1)
    start = date.replace(minute=0,hour=0,second=0,microsecond=0) - utcOffset
    end = date.replace(minute=59,hour=23,second=59,microsecond=0) - utcOffset

    thread = threading.Thread(target=queryRental, args=(start,end))
    thread.start()
    rentalsComplete.wait()

    for doc in rentalResults:
        q.put(doc["clientId"])

    clientThread = threading.Thread(target=queryClient)
    clientThread.start()

    q.join()
    if(len(clientResults) > 0 and len(rentalResults) > 0):
        formatData()
        writeJson(date)

    rentalResults.clear()
    clientResults.clear()

# Function to query rentals due
def queryRental(start, end):
    rentalQuery = db.collection(u'rentals').where(u'billDate',u'>',start).where(u'billDate',u'<',end)
    rentalFetch(rentalQuery.stream())

# Function to query client with rental due
def queryClient():
    while (q.empty() != True):
        ref = q.get()
        clientFetch(ref.get())

# Function called after callback received
def rentalFetch(col_snapshot):
    print(u'Rental callback received query snapshot at:  ')
    data = None
    print(col_snapshot)
    for doc in col_snapshot:
        data = doc.to_dict()
        data['rentalId'] = doc.id
        rentalResults.append(data)
    rentalsComplete.set()

# Function called after callback received
def clientFetch(doc):
    print(u'Client callback received query snapshot at:  ')
    data = None
    print(doc)
    # for doc in col_snapshot:
    data = doc.to_dict()
    data["clientId"] = doc.id
    clientResults.append(data)
    q.task_done()

# Format data to include rentals for each client within client list
def formatData():
    for client in range(len(clientResults)):
        clientResults[client]["rentals"] = []
        for rental in range(len(rentalResults)):
            id = None
            try:
                id = (rentalResults[rental]["clientId"].path).split("/")
            except AttributeError:
                id = rentalResults[rental]["clientId"]
            if(clientResults[client]["clientId"] == id[1]):
                rentalResults[rental]["clientId"] = id[1]
                rentalResults[rental]["startDate"] = rentalResults[rental]["startDate"].strftime("%d/%m/%Y, %H:%M:%S")
                rentalResults[rental]["billDate"] = rentalResults[rental]["billDate"].strftime("%d/%m/%Y, %H:%M:%S")
                rentalResults[rental]["endDate"] = rentalResults[rental]["endDate"].strftime("%d/%m/%Y, %H:%M:%S")
                rentalResults[rental]["assetId"] = (rentalResults[rental]["assetId"].path).split("/")[1]
                clientResults[client]["rentals"].append(rentalResults[rental])
                id = None
                break

def writeJson(date):
    fileName = date.strftime("%d-%m-%Y") + '.json'
    dirname = os.path.dirname(__file__)
    finalName = os.path.join(dirname, '../files/json/' + fileName)
    with open(finalName, 'w') as file:
        json.dump(clientResults, file, indent=4, sort_keys=True)

#    with open(finalName, 'w', encoding='utf-8') as file:
#        json.dump(clientResults, file, ensure_ascii=False, indent=4)
main()