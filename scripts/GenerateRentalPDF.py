import os
import datetime
import json

from fpdf import FPDF

def main():
    dirname = os.path.dirname(__file__)
    folder = os.path.join(dirname, '../files/json/')
    fileList = os.listdir(folder)

    accountDetails = []
    bDetails = os.path.join(os.path.dirname(__file__), '../environment/accountInfo.json')
    with open(bDetails) as bank_file:
        accountDetails = json.load(bank_file)

    clientFullData = []
    for json_file in fileList:
        path = os.path.join(os.path.dirname(__file__), '../files/json/' + json_file)
        with open(path) as file:
            data = json.load(file)
            for newClient in data:
                if(len(clientFullData) >= 1):
                    found = False
                    for curClient in clientFullData:
                        if(newClient['clientId'] == curClient['clientId']):
                            curClient['rentals'].append(newClient['rentals'][0])
                            found = True
                    if(found == False):
                        clientFullData.append(newClient)
                else:
                    clientFullData.append(newClient)
            for client in clientFullData:
                makePDF(client, accountDetails)
                makeJSON(client)
        os.remove(path)

def makePDF(client, accountDetails):
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font('Arial', 'B', 10)
    pdf.cell(0, 10, 'Economusic')
    
    pdf.set_font('Arial', 'B', size=20)
    pdf.cell(0, 10, 'Rental Invoice', 0, 0, "R")
    pdf.ln()
    
    pdf.set_font("Arial", size=9)
    dateText = datetime.datetime.today().strftime("%b %d, %Y")
    fullText = "# Rental Invoice " + dateText
    pdf.cell(0, 5, fullText, 0, 0, "R")
    pdf.ln()
    pdf.cell(0, 5, (client["name"] + " " + client["surname"]), 0, 0, "R")
    pdf.ln()
    pdf.ln()

    pdf.cell(125, 5, "Bill To: ")
    pdf.cell(0, 5, "Date:")
    pdf.cell(0, 5, dateText, 0, 0, "R")
    pdf.ln()

    pdf.set_font('Arial', 'B', 11)
    pdf.cell(125, 5, (client["name"] + " " + client["surname"]))
    pdf.set_font("Arial", size=9)
    pdf.cell(0, 5, "Payment Terms:")
    pdf.cell(0, 5, "EFT", 0, 0, "R")
    pdf.ln()

    pdf.cell(125, 5, "")
    pdf.cell(0, 5, "")
    pdf.cell(0, 5, "", 0, 0, "R")
    pdf.ln()

    pdf.set_fill_color(220,220,220)
    pdf.set_font("Arial", "B", 11)
    pdf.cell(125, 7, "")
    pdf.cell(0, 7, "Balance Due:", 0, 0, 0, True)
    assets = []
    total = 0
    for rental in client["rentals"]:
        total = total + rental["rent"]
        assets.append({"name": rental["assetName"], "qty": 1, "rent": rental["rent"]})

    pdf.cell(0, 7, ("R " + str(total)), 0, 0, "R")
    pdf.ln()
    pdf.ln()

    pdf.set_fill_color(0, 0, 0)
    pdf.set_text_color(255, 255, 255)
    pdf.cell(98, 7, "Item", "LTB", 0, 0, True)
    pdf.cell(40, 7, "Quantity", "TB", 0, 0, True)
    pdf.cell(32, 7, "Rate", "TB", 0, 0, True)
    pdf.cell(20, 7, "Amount", "TRB", 0, 0, True)
    pdf.ln()
    
    pdf.set_font("Arial", size=11)
    pdf.set_text_color(0, 0, 0)
    for asset in assets:
        pdf.cell(98, 6, asset["name"])
        pdf.cell(40, 6, str(asset["qty"]))
        pdf.cell(32, 6, "R " + str(asset["rent"]))
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
    pdf.cell(0, 5, accountDetails["name"])
    pdf.ln()
    pdf.cell(0, 5, accountDetails["bank"])
    pdf.ln()
    pdf.cell(0, 5, accountDetails["num"])
    pdf.ln()

    final = os.path.join(os.path.dirname(__file__), '../files/batched/' + client["clientId"] + '.pdf')
    pdf.output(final, 'F')

def makeJSON(client):
    finalName = os.path.join(os.path.dirname(__file__), '../files/batched/' + client["clientId"] + '.json')
    with open(finalName, 'w') as file:
        json.dump(client, file, indent=4, sort_keys=True)