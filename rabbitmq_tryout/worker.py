import pandas as pd
import json

def get_parent_key(json_data, fields_list):
    print(type(json_data))
    for key in json_data:
        # if the key is of type - list and the elements are of dict type then you can search for the records
        if isinstance(json_data[key], list) and isinstance(json_data[key][0], dict):
            list_of_keys =list(json_data[key][0].keys())
            print(type(list_of_keys))
            if (all(x in list_of_keys for x in fields_list)):
                 return key
    return None


f = open("data.json")
data = json.load(f)

parent = get_parent_key(data, ["d", "tag"])
print(parent)

# df = pd.read_csv("bos2021ModC.csv")
# print(df.drop("level", axis=1))
