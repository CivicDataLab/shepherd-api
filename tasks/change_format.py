import json
import os

import pandas as pd
import pdfkit
from json2xml import json2xml


def csv_to_pdf(data):
    data.to_html("data.html")
    pdfkit.from_file("data.html","pdf_data.pdf")
    os.remove('data.html')


def csv_to_xml(data):
    data_string = data.to_json(orient='records')
    json_data = json.loads(data_string)
    xml_data = json2xml.Json2xml(json_data).to_xml()
    print(xml_data)
    with open('xml_data.xml', 'w') as f:
        f.write(xml_data)


def csv_to_json(data):
    data_string = data.to_json(orient='records')
    # json_data = json.loads(data_string)
    # print(type(json_data))
    # print(json_data)
    print(type(data_string))
    print(data_string)
    with open("json_data.json", "w") as f:
         f.write(data_string)

data = pd.read_csv('E:\git\my_try\shepherd-api\data110.csv')
format = input("To which format u need to change the file\n")
if format == "pdf":
    csv_to_pdf(data)
elif format == "xml":
    csv_to_xml(data)
elif format == "json":
    csv_to_json(data)