k = [{"key": "12", "format": "ok"},
     {"key": '', "format": ""},
     {"key": "1212", "format": "Nok"}]

for d in k:
    if d['key'] == "":
        k.remove(d)
print(k)