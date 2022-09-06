import requests
import json


file_path = 'data110.csv'
file      = open(file_path, 'rb')
variables = {"file": None}  
map       = json.dumps({ "0": ["variables.file"] })


query = f"""
        mutation($file: Upload!) {{update_resource(resource_data: {{
        id:11, title:"demo2", description:"creating demo2",file:   $file, dataset:"5",
        status:"Trans", format:"csv", remote_url:"http://google.in"
        }})
        {{
        success
        errors
        resource {{ id }}
    }}
    }}"""
    
operations = json.dumps({
  "query": query,
  "variables": variables
})



headers = {} 
response = requests.post('http://idpbe.civicdatalab.in/graphql', data = {"operations": operations,"map": map}, files = {"0" : file}, headers=headers)

response_json = json.loads(response.text)
print(response_json)
