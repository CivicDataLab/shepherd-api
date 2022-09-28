schema = [{'key': 'author', 'format': 'string', 'description': 'Sample'},
          {'key': '', 'format': '', 'description': ''},
          {'key': '', 'format': '','description': ''},
          {'key': 'title with price', 'format': 'string', 'description': 'Result of merging columns title & price by pipeline - p500'}]
temp_schema = []
for sc in schema:
    if len(sc['key']) != 0:
        temp_schema.append(sc)

print(temp_schema)