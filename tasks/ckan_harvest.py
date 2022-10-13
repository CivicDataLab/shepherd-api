import re

from pipeline.task import Task
import ckanapi
import math
import requests
import json


def create_resource(resource, dataset_id):
    # payload = {
    #     "query": "mutation createResourceMutation { create_resource(   resource_data: "
    #              "{ title: \"" + resource["name"] + "\","
    #              " description: \" " + resource["description"] + " \","
    #              " remote_url: \" " +
    #              resource["url"] + " \","
    #              " format: \" " + resource["format"] + " \","
    #              " dataset: \" " + dataset_id + "\" } ) "
    #              "{   resource {"
    #              "     id  "
    #              "   } } }",
    #     "variables": {}}
    query = f"""mutation 
        mutation_create_resource
        {{create_resource(
                    resource_data: {{ title:"{resource["name"]}", description:"{resource["description"]}",    
                    dataset: "{dataset_id}", status : "{resource["state"]}",
                    schema: []
                    file_details:{{format: "{resource["format"]}",  
                    remote_url: "{resource["url"]}"
                    }}
                    }})
                    {{
                    resource {{ id }}
                    }}
                    }}
                    """
    print(query)
    response = requests.post("https://idpbe.civicdatalab.in/graphql", json={'query': query})
    print(response.text)
    print("Resource created..........")
    return response


source_url = src_ckan = "https://dataspace.niua.org"
src_dataset_count = 460
src_ckan = ckanapi.RemoteCKAN(src_ckan, apikey='')
pages = math.ceil(src_dataset_count / 100)
geo_mapping = {
    "ugsdc2022": "Other",
    "agartala-smart-city-limited": "Agartala",
    "agra-smart-city-ltd": "Agra",
    "aligarh-smart-city-limited": "Aligarh",
    "amritsar-smart-city-limited": "Amritsar",
    "aurangabad-smart-city-development-corporation-limited": "Aurangabad",
    "bareilly-smart-city-limited": "Bareilly",
    "belagavi-smart-city-limited": "Belagavi",
    "bengaluru-city-traffic-police-btp": "Bengaluru",
    "bengaluru-metropolitan-transport-corporation-bmtc": "Bengaluru",
    "bengaluru-smart-city-limited": "Bengaluru",
    "bhopal-smart-city-development-corporation-limited": "Bhopal",
    "bhubaneswar-smart-city-limited": "Bhubaneswar",
    "biharsharif-smart-city-limited": "Biharsharif",
    "bilaspur-smart-city-limited": "Bilaspur",
    "capital-region-urban-transport": "New Delhi",
    "chandigarh-smart-city-limited": "Chandigarh",
    "chennai-smart-city-limited": "Chennai",
    "dahod-smart-city-development-limited": "Dahod",
    "damu": "Damu",
    "data-meet": "Other",
    "davangere-smart-city-limited": "Davangere",
    "dehradun-smart-city": "Dehradun",
    "directorate-general-of-civil-aviation": "Other",
    "diu-smart-city-limited": "Diu",
    "energy-efficiency-services-limited": "Other",
    "erode-smart-city-limited": "Erode",
    "faridabad-smart-city-limited": "Faridabad",
    "gandhinagarsmartcitydevelopmentlimited": "Gandhinagar",
    "geological-survey-of-india": "Other",
    "greater-chennai-corporation": "Chennai",
    "greater-visakhapatnam-smart-city-corporation-limited": "Visakhapatnam",
    "greater-warangal-smart-city-corporation-limited": "Warangal",
    "gurugram-metropolitan-city-bus-limited": "Gurugram",
    "gwalior-smart-city-development-corporation-limited": "Gwalior",
    "hubballi-dharwad-smart-city-limited": "Hubballi Dharwad",
    "south-central-railway": "Other",
    "imagine-panaji-smart-city-development-limited": "Panaji",
    "indore-smart-city-development-limited": "Indore",
    "indraprastha-institute-of-information-technology-delhi-iiit-delhi-iiit-d": "New Delhi",
    "jabalpur-smart-city-limited": "Jabalpur",
    "jaipur-smart-city-limited": "Jaipur",
    "jalandhar-smart-city-limited": "Jalandhar",
    "jammu-smart-city-limited": "Jammu",
    "jhansi-smart-city-limited": "Jhansi",
    "kakinada-smart-city-corporation-limited": "Kakinada",
    "kanpur-smart-city-limited": "Kanpur",
    "karimnagar-smart-city-corporation-limited": "Karimnagar",
    "kavaratti-smart-city-limited": "Kavaratti",
    "kochi-metro-rail-limited": "Kochi",
    "kohima-smart-city-development-limited": "Kohima",
    "kolkata-municipal-corporation-kmc": "Kolkata",
    "kota-smart-city-limited": "Kota",
    "lucknow-smart-city-limited": "Lucknow",
    "ludhiana-smart-city-limited": "Ludhiana",
    "madurai-smart-city-limited": "Madurai",
    "mangaluru-smart-city-limited": "Mangaluru",
    "muzaffarpur-smart-city-limited": "Muzaffarpur",
    "nagpur-smart-and-sustainable-city-development-corporation-limited": "Nagpur",
    "namchi-smart-city-limited": "Namchi",
    "nava-raipur-atal-nagar-smart-city-corporation-limited": "Raipur",
    "newtown-kolkata-green-smart-city-corporation-limited": "Kolkata",
    "pasighat-smart-city-development-corporation-limited": "Pasighat",
    "pimpri-chinchwad-smart-city-limited": "Chinchwad",
    "port-blair-smart-projects-limited": "Port Blair",
    "pryagraj-smart-city-limited": "Pryagraj",
    "pune-mahanagar-parivahan-mahamandal-ltd": "Pune",
    "pune-smart-city-development-corporation-limited": "Pune",
    "raipur-smart-city-limited": "Raipur",
    "roadmetrics": "Other",
    "roads-and-buildings-department-telangana-state": "Telangana",
    "rourkela-smart-city-limited": "Rourkela",
    "sagar-smart-city-limited": "Other",
    "saharanpur-smart-city-limited": "Saharanpur",
    "salem-smart-city-limited": "Salem",
    "satna-smart-city-development-limited": "Satna",
    "shillong-smart-city-limited": "Shillong",
    "shivamogga-smart-city-limited": "Shivamogga",
    "silvassa-smart-city-limited": "Silvassa",
    "smart-city-ahmedabad-development-limited": "Ahmedabad",
    "smart-city-thiruvananthapuram-limited": "Thiruvananthapuram",
    "smart-kalyan-dombivli-development-corporation-limited": "Dombivli",
    "solapur-city-development-corporation-limited": "Solapur",
    "south-central-railways": "Other",
    "srinagar-smart-city": "Srinagar",
    "suratsmartcity": "Surat",
    "thane-smart-city-limited": "Thane",
    "thanjavur-smart-city-limited": "Thanjavur",
    "thoothukudi-smart-city-limited": "Thoothukudi",
    "tiruchirappalli-smart-city-limited": "Tiruchirappalli",
    "tirunelveli-smart-city-limited": "Tirunelveli",
    "tiruppur-smart-city-limited": "Tiruppur",
    "tumakuru-smart-city-limited": "Tumakuru",
    "udaipur-smart-city-limited": "Udaipur",
    "ujjain-smart-city-limited": "Ujjain",
    "urban-datasets": "Other",
    "vadodara-smart-city": "Vadodara",
    "varanasi-smart-city-limited": "Varanasi",
    "niua" : "Other"
}
catalog_id = "1"
package_list = src_ckan.action.current_package_list_with_resources(limit=100, page=1)
# TODO tags - take out all tags from packagelist[tags][name].
# TODO sectors - take out tags and filter out the alphanumeric. then apply if else based on tag name.
# TODO geo - keep old logic for now.
# TODO access_type - set Open if is_access from ckan is True else Restricted.
# TODO remote_issued and remote_modified are metadata_created and metadata_modified respectively
# TODO catalog in graphql - replace with catalog_name taken as i/p.
# TODO


def get_tags_list(tags:list):
    tag_names = []
    for dict in tags:
        tag_names.append(dict["name"])
    tag_names = json.dumps(tag_names)
    return tag_names


def get_sector(tags:str):
    sector = "Other"
    tags_list = json.loads(tags)
    tags_list = list(map(str.lower, tags_list))
    for tag in tags_list:
        if re.search(r'\d', tag):
            tags_list.remove(tag)
    if "transit" in tags_list or "transport" in tags_list:
        sector = "Transport"
    elif 'environment' in tags_list:
        sector = "Environment and Forest"
    elif 'finance' in tags_list:
        sector = "Finance"
    return json.dumps([sector])


for page in range(1, 2):
    package_list = src_ckan.action.current_package_list_with_resources(limit=100, page=page)
    for package in package_list:
        print(package)
        # package = package_list[0]
        sector = 'Transport'
        org = package['organization']
        try:
            geo = geo_mapping[org['name']]
        except:
            geo = "Others"
        tags = package['tags']
        tags_list = get_tags_list(tags)
        sector = get_sector(tags_list)
        pkg_license = package['license_id'] if 'license_id' in package.keys() else 'notspecified'
        access_type = "Open" if package['isopen'] == "True" else "Restricted"
        issued = package['metadata_created']
        modified = package['metadata_modified']

        query = f"""mutation
        {{
            create_dataset(dataset_data:{{
                title: "{package["name"]}",
                description: "{package["title"]}",
                sector_list: {sector},
                geo_list: "{geo}",
                remote_issued: "{issued}", remote_modified: "{modified}",
                tags_list: {tags_list},
                catalog: "{catalog_id}",
                status: "{package['state']}"
                dataset_type: DATASET

            }})
            {{
            dataset{{
                id
            }}
            }}
        }}
"""
        print(query)
        response = requests.post("https://idpbe.civicdatalab.in/graphql", json={'query': query})
        print(response.text)
        dataset_id = json.loads(response.text)['data']['create_dataset']['dataset']['id']

        resources = package['resources']

        for resource in resources:
            create_resource(resource, dataset_id)


# CkanTOIDP(src_ckan="https://dataspace.niua.org", src_dataset_count=460, dest_organization="")._execute()
