import os
import datetime
import json
import email, smtplib, ssl
import threading
import queue

from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

accountInfo = {}
infoPath = os.path.join(os.path.dirname(__file__), '../environment/emailinfo.json')
with open(infoPath) as file:
    accountInfo = json.load(file)

path = os.path.join(os.path.dirname(__file__), '../environment/credentials.json')
cred = credentials.Certificate(path)
firebase_admin.initialize_app(cred)
db = firestore.client()

q = queue.Queue()

totalRent = []

def main():
    dirname = os.path.dirname(__file__)
    folder = os.path.join(dirname, '../files/batched/')
    fileList = os.listdir(folder)

    fileNames = []
    for json_file in fileList:
        temp = json_file.split('.')
        found = False
        for item in fileNames:
            if(item == temp[0]):
                found = True
                break
        if(found == False):
            fileNames.append(temp[0])

    for client in fileNames:
        getClientDetails(client)
    
    complete(fileNames)

def getClientDetails(clientId):
    path = os.path.join(os.path.dirname(__file__), '../files/batched/' + clientId + '.json')
    clientData = None
    with open(path) as file:
        clientData = json.load(file)
    try:
        if(clientData["sent"] == False):
            createEmail(clientData)
        else:
            archiveFiles(clientId)
    except KeyError:
        createEmail(clientData)

def createEmail(client):
    subject = "Invoice"
    body =  "Good morning, \n\n Please find your attached Economusic rental invoice for the month."
    pdfName = 'Economusic Invoice.pdf'
    message = MIMEMultipart()
    message["From"] = accountInfo["login"]
    message["To"] = client["email"]
    message["Subject"] = subject
    message.attach(MIMEText(body, "plain"))
    check = False
    with open(os.path.join(os.path.dirname(__file__), '../files/batched/' + client["clientId"] + '.pdf'), "rb") as attachment:
        part = MIMEBase("application", "octet-stream")
        part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f"attachment; filename={pdfName}",)
        message.attach(part)
        text = message.as_string()
        check = sendEmail(client["email"],text)

    client["sent"] = check
    with open(os.path.join(os.path.dirname(__file__), '../files/batched/' + client["clientId"] + '.json'), 'w', encoding='utf-8') as json_file:
        json.dump(client, json_file, ensure_ascii=False, indent=4)

def sendEmail(clientEmail, message):
    # Not secure at all
    try:
        with smtplib.SMTP(accountInfo["server"], accountInfo["port"]) as server:
            server.login(accountInfo["login"], accountInfo["password"])
            server.sendmail(accountInfo["login"], clientEmail, message)
        return True
    except:
        return False

def complete(fileNames):
    clientList = []
    for file in fileNames:
        path = os.path.join(os.path.dirname(__file__), '../files/batched/' + file + '.json')
        try:
            with open(path) as file:
                clientList.append(json.load(file))
        except FileNotFoundError:
            pass

    for client in clientList:
        total = 0
        for rental in client["rentals"]:
            total = total - rental["rent"]
        totalRent.append({"client": client["clientId"], "total": total})
        try:
            if(client["sent"] == True):
                q.put(db.collection(u'clients').document(client["clientId"]))
            else:
                pass
        except KeyError:
            client["sent"] = False

    clientThread = threading.Thread(target=currentClient)
    clientThread.start()
    q.join()

def currentClient():
    while (q.empty() != True):
        ref = q.get()
        ref.on_snapshot(updateClient)

def updateClient(col_snapshot, changes, read_time):
    print(u'Callback received query snapshot at:  ', read_time)
    data = None
    for doc in col_snapshot:
        data = doc.to_dict()
        data["clientId"] = doc.id
        for total in totalRent:
            if(data["clientId"] == total["client"]):
                final = data["balance"] + total["total"]
                db.collection(u'clients').document(data["clientId"]).update({
                        u'balance': final
                })
                archiveFiles(data["clientId"])
                break
    q.task_done()

def archiveFiles(fileName):
    dateToday = datetime.datetime.now()
    batchPath = "../files/batched/"
    archivePath = "../files/archive/" + dateToday.strftime("%Y") + "/" + dateToday.strftime("%m") + "/" + dateToday.strftime("%d")
    directoryName = os.path.dirname(__file__)
    try:
        os.makedirs(os.path.join(directoryName, archivePath))
    except FileExistsError:
        pass

    os.rename(os.path.join(directoryName , batchPath + fileName + '.json'), os.path.join(directoryName ,archivePath + "/" + fileName + '.json'))
    os.rename(os.path.join(directoryName , batchPath + fileName + '.pdf'), os.path.join(directoryName ,archivePath + "/" + fileName + '.pdf'))
    