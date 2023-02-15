import pandas as pd
data_url = "https://cdn.wsform.com/wp-content/uploads/2020/06/county_uk.csv"
all_data = pd.read_csv(data_url)
print(all_data)