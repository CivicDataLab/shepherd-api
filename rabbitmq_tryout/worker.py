import json

import requests

cookies = {
    'ASP.NET_SessionId': 'q0abtq2zxuixfhnkas3foxfo',
}

headers = {
    'Accept': 'application/json, text/javascript, /; q=0.01',
    'Accept-Language': 'en-US,en;q=0.9',
    'Connection': 'keep-alive',
    # Already added when you pass json=
    # 'Content-Type': 'application/json',
    # 'Cookie': 'ASP.NET_SessionId=q0abtq2zxuixfhnkas3foxfo',
    'Origin': 'https://onlineasdma.assam.gov.in',
    'Referer': 'https://onlineasdma.assam.gov.in/gis/Forum/Question/MODULES/GIS_Console/GIS_Console.aspx',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36 Edg/105.0.1343.42',
    'sec-ch-ua': '"Microsoft Edge";v="105", " Not;A Brand";v="99", "Chromium";v="105"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
}

json_data = {
    'id': '59',
    'districtid': '1,2,28,3,4,29,5,6,8,21,10,11,12,30,13,14,15,16,17,18,31,20,22,23,24,25,32,27,33,19,26,7,9',
    'layerids': '50',
}

response = requests.post('https://onlineasdma.assam.gov.in/asdmaservices/Service2.svc/Fit_Layer', cookies=cookies, headers=headers, json=json_data).text
print()