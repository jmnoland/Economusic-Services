import os
import json
import datetime
import threading

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from fpdf import FPDF

import email, smtplib, ssl
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

OrderThread = threading.Event()

class QuoteOrderInvoice():

    def __init__(self):
        dirname = os.path.dirname(__file__)
        filename = os.path.join(dirname, '../environment/credentials.json')

        self.__accountDetails = []
        bDetails = os.path.join(os.path.dirname(__file__), '../environment/accountInfo.json')
        with open(bDetails) as bank_file:
            self.__accountDetails = json.load(bank_file)

        cred = credentials.Certificate(filename)
        app = firebase_admin.initialize_app(cred)
        self.__db = firestore.client(app=app)

        thread = threading.Thread(target=self.queryOrders)
        thread.start()
        OrderThread.wait()

    def queryOrders(self):
        doc_ref = self.__db.collection(u'orders').where(u'sent',u'==',False)
        doc_ref.on_snapshot(self.on_snapshot)

    def on_snapshot(self, doc_snapshot, changes, read_time):
        for doc in doc_snapshot:
            print(u'Received document snapshot: {}'.format(doc.id))
            data = doc.to_dict()
            data['orderId'] = doc.id
            self.makePDF(data)

    def makePDF(self, data):
        pdf = FPDF()
        pdf.add_page()
        pdf.set_font('Arial', 'B', 10)
        pdf.cell(0, 10, 'Economusic')
        
        pdf.set_font('Arial', 'B', size=20)
        text = 'Order quote'
        if(data['quote'] == False):
            text = 'Order invoice'
        pdf.cell(0, 10, text, 0, 0, "R")
        pdf.ln()
        
        pdf.set_font("Arial", size=9)
        dateText = datetime.datetime.today().strftime("%b %d, %Y")
        fullText = "# " + text + " " + dateText
        pdf.cell(0, 5, fullText, 0, 0, "R")
        pdf.ln()
        pdf.cell(0, 5, data['name'], 0, 0, "R")
        pdf.ln()
        pdf.ln()

        pdf.cell(125, 5, "Bill To: ")
        pdf.cell(0, 5, "Date:")
        pdf.cell(0, 5, dateText, 0, 0, "R")
        pdf.ln()

        pdf.set_font('Arial', 'B', 11)
        pdf.cell(125, 5, data['name'])
        pdf.set_font("Arial", size=9)
        pdf.cell(0, 5, "Payment Terms:")
        pdf.cell(0, 5, "EFT", 0, 0, "R")
        pdf.ln()

        pdf.set_fill_color(220,220,220)
        pdf.set_font("Arial", "B", 11)
        pdf.cell(125, 7, "")
        pdf.cell(0, 7, "Reference Number:", 0, 0, 0, True)
        pdf.cell(0, 7, data['reference'], 0, 0, "R")
        pdf.ln()

        pdf.cell(125, 5, "")
        pdf.cell(0, 5, "")
        pdf.cell(0, 5, "", 0, 0, "R")
        pdf.ln()

        if(data['quote'] == False):
            pdf.set_fill_color(220,220,220)
            pdf.set_font("Arial", "B", 11)
            pdf.cell(125, 7, "")
            pdf.cell(0, 7, "Balance Due:", 0, 0, 0, True)

        assets = []
        total = 0
        for item in data:
            if("orderItem" in item):
                total = total + (data[item][1] * data[item][2])
                assets.append({"name": data[item][0], "qty": data[item][1], "rent": data[item][2]})

        if(data['quote'] == False):
            pdf.cell(0, 7, ("R " + str(total)), 0, 0, "R")

        pdf.ln()
        pdf.ln()

        pdf.set_fill_color(0, 0, 0)
        pdf.set_text_color(255, 255, 255)
        pdf.cell(98, 7, "Item", "LTB", 0, 0, True)
        pdf.cell(62, 7, "Quantity", "TB", 0, 0, True)
        pdf.cell(30, 7, "Amount each", "TRB", 0, 0, True)
        pdf.ln()
        
        pdf.set_font("Arial", size=11)
        pdf.set_text_color(0, 0, 0)
        for asset in assets:
            pdf.cell(98, 6, asset["name"])
            pdf.cell(72, 6, str(asset["qty"]))
            pdf.cell(20, 6, "R " + str(asset["rent"]))
            pdf.ln()
        pdf.ln()
        pdf.ln()

        pdf.cell(125, 5, "")
        pdf.cell(0, 5, "Subtotal:")
        pdf.cell(0, 5, "R " + str(total), 0, 0, "R")
        pdf.ln()

        pdf.cell(125, 5, "")
        pdf.cell(0, 5, "Total:")
        pdf.cell(0, 5, "R " + str(total), 0, 0, "R")
        pdf.ln()

        pdf.cell(0, 10, "Notes")
        pdf.ln()
        pdf.cell(0, 5, self.__accountDetails["name"])
        pdf.ln()
        pdf.cell(0, 5, self.__accountDetails["bank"])
        pdf.ln()
        pdf.cell(0, 5, self.__accountDetails["num"])
        pdf.ln()

        final = os.path.join(os.path.dirname(__file__), '../files/orders/' + data["orderId"] + '.pdf')
        pdf.output(final, 'F')
        self.createEmail(data)

    def createEmail(self, data):
        self.accountInfo = {}
        infoPath = os.path.join(os.path.dirname(__file__), '../environment/emailinfo.json')
        with open(infoPath) as file:
            self.accountInfo = json.load(file)

        text = 'quote'
        if(data['quote'] == False):
            text = 'invoice'

        morAft = "afternoon"
        if(datetime.datetime.today().hour < 12):
            morAft = "morning"

        subject = text
        body =  "Good " + morAft + ", \n\n Please find your attached Economusic Order " + text + "."
        pdfName = 'Economusic_' + text + '.pdf'
        message = MIMEMultipart()
        message["From"] = self.accountInfo["login"]
        message["To"] = data["email"]
        message["Subject"] = subject
        message.attach(MIMEText(body, "plain"))
        with open(os.path.join(os.path.dirname(__file__), '../files/orders/' + data["orderId"] + '.pdf'), "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", "attachment; filename={0}".format(pdfName))
            message.attach(part)
            textMessage = message.as_string()
        self.sendEmail(data, textMessage)

    def sendEmail(self, data, message):
        # Not secure at all
        server = None
        try:
            server = smtplib.SMTP(self.accountInfo["server"], self.accountInfo["port"])
            server.login(self.accountInfo["login"], self.accountInfo["password"])
            server.sendmail(self.accountInfo["login"], data['email'], message)
            server.quit()
            self.updateOrder(data)
        except:
            try:
                server.quit()
            except:
                pass
    
    def updateOrder(self, data):
        self.__db.collection(u'orders').document(data["orderId"]).update({
                u'sent': True
        })
        self.archiveFile(data['orderId'])

    def archiveFile(self, fileName):
        dateToday = datetime.datetime.now()
        orderPath = "../files/orders/"
        archivePath = "../files/orders/" + dateToday.strftime("%Y") + "/" + dateToday.strftime("%m") + "/" + dateToday.strftime("%d")
        directoryName = os.path.dirname(__file__)
        try:
            os.makedirs(os.path.join(directoryName, archivePath))
        except FileExistsError:
            pass

        try:
            os.rename(
                os.path.join(directoryName , orderPath + fileName + '.pdf'), 
                os.path.join(directoryName ,archivePath + "/" + fileName + dateToday.strftime("%H-%M-%S") + '.pdf')
                )
            os.remove(os.path.join(directoryName , orderPath + fileName + '.pdf'))
        except:
            pass
