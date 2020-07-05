import os
import datetime
from dateutil.relativedelta import relativedelta
import json
import email, smtplib, ssl
import threading

try:
    import queue
except ImportError:
    import Queue as queue

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
app = firebase_admin.initialize_app(cred, name="FinalizeApp")
db = firestore.client(app=app)

q = queue.Queue()

totalRent = []
archiveList = []
mailSent = [False]

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
    if(len(archiveList) > 0):
        if mailSent[0]:
            try:
                finalEmail()
            except FileNotFoundError:
                pass
        archiveFiles()

    archiveList.clear()
    totalRent.clear()

def getClientDetails(clientId):
    path = os.path.join(os.path.dirname(__file__), '../files/batched/' + clientId + '.json')
    clientData = None
    with open(path) as file:
        clientData = json.load(file)
    try:
        if(clientData["sent"] == False):
            createEmail(clientData)
        else:
            archiveList.append(clientId)
    except KeyError:
        createEmail(clientData)

def createEmail(client):
    subject = "Economusic Invoice"
    morAft = "afternoon"
    if(datetime.datetime.today().hour < 12):
        morAft = "morning"

    body =  "Good " + morAft + ", \n\nPlease find your attached rental invoice for the month.\n\nRegards\nEconomusic"
    pdfName = 'Economusic_Invoice.pdf'
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
        part.add_header("Content-Disposition", "attachment; filename={0}".format(pdfName))
        message.attach(part)
        text = message.as_string()
        check = sendEmail(client["email"],text)
        if check:
            mailSent[0] = True
            try:
                for cc in client["ccEmails"]:
                    sendEmail(cc, text)
            except KeyError:
                pass

    client["sent"] = check
    client["sentTime"] = datetime.datetime.today().strftime("%d/%m/%Y, %H:%M:%S")
    with open(os.path.join(os.path.dirname(__file__), '../files/batched/' + client["clientId"] + '.json'), 'w') as json_file:
        json.dump(client, json_file, indent=4, sort_keys=True)

def sendEmail(clientEmail, message):
    # Not secure at all
    server = None
    try:
        # with smtplib.SMTP(accountInfo["server"], accountInfo["port"]) as server:
        #    server.login(accountInfo["login"], accountInfo["password"])
        #    server.sendmail(accountInfo["login"], clientEmail, message)
        server = smtplib.SMTP(accountInfo["server"], accountInfo["port"])
        server.login(accountInfo["login"], accountInfo["password"])
        server.sendmail(accountInfo["login"], clientEmail, message)
        server.quit()
        return True
    except:
        try:
            server.quit()
        except:
            pass
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
        updateDates = []
        for rental in client["rentals"]:
            total = total - rental["rent"]
            updateDates.append({
                "rentalId": rental["rentalId"], 
                "billDate": datetime.datetime.strptime(rental["billDate"], '%d/%m/%Y, %H:%M:%S'),
                "endDate": datetime.datetime.strptime(rental["endDate"], '%d/%m/%Y, %H:%M:%S')
                })
        totalRent.append({"client": client["clientId"], "total": total, "billDates": updateDates})
        try:
            if(client["sent"] == True and (client["clientId"] not in archiveList)):
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
        updateClient(ref.get())

def updateClient(doc):
    data = doc.to_dict()
    data["clientId"] = doc.id
    for total in totalRent:
        if(data["clientId"] == total["client"]):
            final = data["balance"] + total["total"]
            db.collection(u'clients').document(data["clientId"]).update({
                    u'balance': final
            })
            archiveList.append(data["clientId"])
            updateRentals(total)
            break
    q.task_done()

def updateRentals(allInfo):
    for rental in allInfo["billDates"]:
        nextMonth = rental["billDate"] + relativedelta(months=+1)
        if not(nextMonth > (rental["endDate"] + relativedelta(months=+1))):
            db.collection(u'rentals').document(rental['rentalId']).update({
                u'billDate': nextMonth
            })

def finalEmail():
    subject = "Summary of invoiced clients"
    body =  "The following clients have been invoiced today:\n"
    message = MIMEMultipart()
    message["From"] = accountInfo["login"]
    message["To"] = accountInfo["summary"]
    message["Subject"] = subject
    for file in archiveList:
        path = os.path.join(os.path.dirname(__file__), '../files/batched/' + file + '.json')
        with open(path, 'r') as json_file:
            client = json.load(json_file)
            if client["sent"]:
                body = body + ("\t - " + client["name"] + " " + client["surname"] + "\n")
    message.attach(MIMEText(body, "plain"))
    try:
        server = smtplib.SMTP(accountInfo["server"], accountInfo["port"])
        server.login(accountInfo["login"], accountInfo["password"])
        server.sendmail(accountInfo["login"], accountInfo["login"], message.as_string())
        server.quit()
    except:
        try:
            server.quit()
        except:
            pass

def archiveFiles():
    for fileName in archiveList:
        dateToday = datetime.datetime.now()
        batchPath = "../files/batched/"
        archivePath = "../files/archive/" + dateToday.strftime("%Y") + "/" + dateToday.strftime("%m") + "/" + dateToday.strftime("%d")
        directoryName = os.path.dirname(__file__)
        try:
            os.makedirs(os.path.join(directoryName, archivePath))
        except FileExistsError:
            pass

        try:
            os.rename(os.path.join(directoryName , batchPath + fileName + '.json'), os.path.join(directoryName ,archivePath + "/" + fileName + '.json'))
            os.rename(os.path.join(directoryName , batchPath + fileName + '.pdf'), os.path.join(directoryName ,archivePath + "/" + fileName + '.pdf'))
            os.remove(os.path.join(directoryName , batchPath + fileName + '.json'))
            os.remove(os.path.join(directoryName , batchPath + fileName + '.pdf'))
        except:
            pass