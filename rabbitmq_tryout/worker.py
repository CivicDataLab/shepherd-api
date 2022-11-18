import pandas as pd
import json

def get_parent_key(json_data, fields_list):
    str_data = json.dumps(json_data)
    json_data = json.loads(str_data.__dict__)

    for item in json_data:
        print("item is...", item, " and the type is....", type(item))
        item = json.load(item)
        print("type is now...", type(item))
        if isinstance(item, list):
            print("item keys are-----", item[0].keys())
        if isinstance(item, list) and fields_list in [item[0].keys()]:
            return item



with open('data.json') as f:
    d = json.load(f)
parent = get_parent_key(d, ["d", "initiationtype"])
print(parent)

# df = pd.read_csv("bos2021ModC.csv")
# print(df.drop("level", axis=1))
