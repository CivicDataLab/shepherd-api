import random
import re

import pandas as pd
import json
import pandas as pd
with open('data.json', 'rb') as f:
    data = json.load(f)
# df = pd.DataFrame(data)
records = data['records']
print(records)
df = pd.DataFrame(records)
print(df.columns)