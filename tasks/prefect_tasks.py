import math
import os
import re

import ckanapi
import pandas as pd
import pdfkit
import requests
from json2xml import json2xml
from pandas.io.json import build_table_schema
from prefect import task, flow
from task_utils import *


@task
def skip_column(context, pipeline, task_obj):
    column = context['columns']
    col = column
    if not isinstance(column, list):
        column = list()
        column.append(col)
    try:
        pipeline.data = pipeline.data.drop(column, axis=1)
        for col in column:
            for sc in pipeline.schema:
                if sc['key'] == col:
                    sc['key'] = ""
                    sc['format'] = ""
                    sc['description'] = ""
        set_task_model_values(task_obj, pipeline)
    except Exception as e:
        send_error_to_prefect_cloud(e)
        task_obj.status = "Failed"
        task_obj.save()


@task
def merge_columns(context, pipeline, task_obj):
    column1, column2, output_column = context['column1'], context['column2'], context['output_column']
    separator = context['separator']

    try:
        pipeline.data[output_column] = pipeline.data[column1].astype(str) + separator + pipeline.data[column2] \
            .astype(str)
        pipeline.data = pipeline.data.drop([column1, column2], axis=1)

        """ setting up the schema after task"""
        data_schema = pipeline.data.convert_dtypes(infer_objects=True, convert_string=True,
                                                   convert_integer=True, convert_boolean=True, convert_floating=True)
        names_types_dict = data_schema.dtypes.astype(str).to_dict()
        new_col_format = names_types_dict[output_column]
        for sc in pipeline.schema:
            if sc['key'] == column1:
                sc['key'] = ""
                sc['format'] = ""
                sc['description'] = ""
            if sc['key'] == column2:
                sc['key'] = ""
                sc['format'] = ""
                sc['description'] = ""
        pipeline.schema.append({
            "key": output_column, "format": new_col_format,
            "description": "Result of merging columns " + column1 + " & " + column2 + " by pipeline - "
                           + pipeline.model.pipeline_name
        })
        set_task_model_values(task_obj, pipeline)
    except Exception as e:
        send_error_to_prefect_cloud(e)
        task_obj.status = "Failed"
        task_obj.save()


@task
def anonymize(context, pipeline, task_obj):
    # TODO - decide on the context contents
    to_replace = context['to_replace']
    replace_val = context['replace_val']
    col = context['column']
    try:
        df_updated = pipeline.data[col].astype(str).str.replace(re.compile(to_replace, re.IGNORECASE), replace_val)
        df_updated = df_updated.to_frame()
        pipeline.data[col] = df_updated[col]
        data_schema = pipeline.data.convert_dtypes(infer_objects=True, convert_string=True,
                                                   convert_integer=True, convert_boolean=True, convert_floating=True)
        names_types_dict = data_schema.dtypes.astype(str).to_dict()

        for sc in pipeline.schema:
            if sc['key'] == col:
                sc['format'] = names_types_dict[col]
        set_task_model_values(task_obj, pipeline)
    except Exception as e:
        send_error_to_prefect_cloud(e)
        task_obj.status = "Failed"
        task_obj.save()


@task
def change_format(context, pipeline, task_obj):
    file_format = context['format']
    result_file_name = pipeline.model.pipeline_name
    dir = "format_changed_files/"
    if file_format == "xml" or file_format =="XML":
        try:
            data_string = pipeline.data.to_json(orient='records')
            json_data = json.loads(data_string)
            xml_data = json2xml.Json2xml(json_data).to_xml()
            with open(dir+result_file_name + '.xml', 'w') as f:
                f.write(xml_data)
            set_task_model_values(task_obj, pipeline)
        except Exception as e:
            send_error_to_prefect_cloud(e)
            task_obj.status = "Failed"
            task_obj.save()
    elif file_format == "pdf" or file_format == "PDF":
        try:
            pipeline.data.to_html("data.html")
            pdfkit.from_file("data.html", dir+result_file_name + ".pdf")
            os.remove('data.html')
            set_task_model_values(task_obj, pipeline)
        except Exception as e:
            send_error_to_prefect_cloud(e)
            task_obj.status = "Failed"
            task_obj.save()
    elif file_format == "json" or file_format == "JSON":
        try:
            data_string = pipeline.data.to_json(orient='records')
            with open(dir + result_file_name + ".json", "w") as f:
                f.write(data_string)
            set_task_model_values(task_obj, pipeline)
        except Exception as e:
            send_error_to_prefect_cloud(e)
            task_obj.status = "Failed"
            task_obj.save()

@task
def aggregate(context, pipeline, task_obj):
    print("inside aggregate")
    index = context['index']
    columns = context['columns']
    values = context['values']
    columns = columns.split(",")
    values = values.split(",")
    try:
        pipeline.data = pd.pivot(pipeline.data, index=index, columns=columns, values=values)
        inferred_schema = build_table_schema(pipeline.data)
        fields = inferred_schema['fields']
        new_schema = []
        for field in fields:
            key = field['name']
            description = ""
            format = field['type']
            for sc in pipeline.schema:
                if sc['key'] == key or sc['key'] == key[0]:
                    description = sc['description']
            if isinstance(key, tuple):
                key = " ".join(map(str, key))
            new_schema.append({"key": key, "format": format, "description": description})
        pipeline.schema = new_schema
        set_task_model_values(task_obj, pipeline)
    except Exception as e:
        send_error_to_prefect_cloud(e)
        task_obj.status = "Failed"
        task_obj.save()


@task
def query_data_resource(context, pipeline, task_obj):
    columns = context['columns']
    num_rows = context["rows"]
    columns = columns.split(",")
    if len(columns) == 1 and len(columns[0]) == 0:
        column_selected_df = pipeline.data
    else:
        column_selected_df = pipeline.data.loc[:, pipeline.data.columns.isin(columns)]
        for sc in pipeline.schema:
            if sc["key"] not in columns:
                sc["key"] = ""
                sc["format"] = ""
                sc["description"] = ""
    # if row length is not specified return all rows
    if num_rows == "" or int(num_rows) > len(column_selected_df):
        final_df = column_selected_df
    else:
        num_rows_int = int(num_rows)
        final_df = column_selected_df.iloc[:num_rows_int]
    pipeline.data = final_df
    set_task_model_values(task_obj, pipeline)


@task
def ckan_harvest(context, pipeline, task_obj):
    def create_resource(resource, dataset_id):
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
        return response

    def get_tags_list(tags: list):
        tag_names = []
        for dict in tags:
            tag_names.append(dict["name"])
        tag_names = json.dumps(tag_names)
        return tag_names

    def get_sector(tags: str):
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
    print("inside ckan harvest...")
    src_ckan = context['src_url']
    src_dataset_count = int(context['dataset_count'])
    try:
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
            "niua": "Other"
        }
        catalog_id = "1"
        package_list = src_ckan.action.current_package_list_with_resources(limit=100, page=1)

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
                response = requests.post("https://idpbe.civicdatalab.in/graphql", json={'query': query})
                dataset_id = json.loads(response.text)['data']['create_dataset']['dataset']['id']

                resources = package['resources']

                for resource in resources:
                    create_resource(resource, dataset_id)
    except Exception as e:
        send_error_to_prefect_cloud(e)
        task_obj.status = "Failed"
        task_obj.save()



@flow
def pipeline_executor(pipeline):
    print("setting ", pipeline.model.pipeline_name, " status to In Progress")
    pipeline.model.status = "In Progress"
    print(pipeline.model.status)
    pipeline.model.save()
    tasks_objects = pipeline._commands
    func_names = get_task_names(tasks_objects)
    contexts = get_task_contexts(tasks_objects)
    try:
        for i in range(len(func_names)):
            globals()[func_names[i]](contexts[i], pipeline, tasks_objects[i])
    except Exception as e:
        raise e
    # if any of the tasks is failed, set pipeline status as - Failed
    for each_task in tasks_objects:
        if each_task.status == "Failed":
            pipeline.model.status = "Failed"
            pipeline.model.save()
            break
    # if none of the tasks failed, set pipeline status as - Done
    if pipeline.model.status != "Failed":
        pipeline.model.status = "Done"
        pipeline.model.save()
    pipeline.model.output_id = str(pipeline.model.pipeline_id) + "_" + pipeline.model.status
    print("Data after pipeline execution\n", pipeline.data)
    pipeline.model.save()
    return
