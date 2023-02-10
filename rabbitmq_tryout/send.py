import json
import random

option = "replace_all"
special_char = "*"
n = "2"



def anonymize_a_key(d, change_key):
    for key in list(d.keys()):
        if key == change_key:
            print("got change key..")
            val = d[key]
            print("value type is...", type(val))
            if isinstance(val, int):
                val = str(val)
            if isinstance(val, str):
                if option == "replace_all":
                    if special_char == "random":
                        replace_val = "".join(random.choices("!@#$%^&*()<>?{}[]~`", k=len(val)))
                        d[key] = replace_val
                    else:
                        replace_val = special_char * len(val)
                        d[key] = replace_val
                elif option == "replace_nth":
                    # n = context.get('n')
                    # n = int(n) - 1
                    # if special_char == "random":
                    #     replacement = "".join(random.choices("!@#$%^&*()<>?{}[]~`", k=1))
                    #     replace_val = val[0:int(n)] + replacement + val[int(n) + 1:]
                    #     d[key] = replace_val
                    # else:
                    #     replace_val = val[0:int(n)] + special_char + val[int(n) + 1:]
                    #     d[key] = replace_val
                    # n = context.get('n')
                    n = int(n)  # - 1
                    if special_char == "random":
                        replacement = "".join(random.choices("!@#$%^&*()<>?{}[]~`", k=1))

                        for i in range((n - 1), len(val) + (n - 1), n):
                            val = val[: i] + replacement + val[i + 1:]
                            # for i in range(0, len(val), int(n)):
                        #     val = val[ : i] + replacement + val[i + 1: ] 
                        # replace_val = val[0:int(n)] + replacement + val[int(n) + 1:]
                        d[key] = val
                    else:
                        for i in range((n - 1), len(val) + (n - 1), n):
                            val = val[: i] + special_char + val[i + 1:]
                            # for i in range(0, len(val), int(n)):
                        #     val = val[ : i] + special_char + val[i + 1: ] 
                        d[key] = val
                elif option == "retain_first_n":
                    if special_char == "random":
                        replacement = "".join(random.choices("!@#$%^&*()<>?{}[]~`", k=(len(val) - int(n))))
                        replace_val = val[:int(n)] + replacement
                        d[key] = replace_val
                    else:
                        replace_val = val[:int(n)] + (special_char * (len(val) - int(n)))
                        d[key] = replace_val
            else:
                anonymize_col(d[key], change_key)
        else:
            print("in else")
            print(d[key])
            anonymize_col(d[key], change_key)


def anonymize_col(d, change_key):
    if isinstance(d, dict):
        anonymize_a_key(d, change_key)
    if isinstance(d, list):
        print("here as I got list")
        for each in d:
            if isinstance(each, dict):
                anonymize_a_key(each, change_key)


    return d

data_file = open("data.json")
data = json.load(data_file)
data_str = json.dumps(data)
data = json.loads(data_str)
final_d = anonymize_col(data, "order")
json_obj = json.dumps(final_d)
print("writing")
with open("transformed.json", "w") as outfile:
    outfile.write(json_obj)