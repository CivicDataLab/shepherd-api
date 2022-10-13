import os
import sys
cur_dir = os.getcwd()
result_file_name = "abc"
xml_data = "xml data"
os.chdir("../format_changed_files")
with open(result_file_name+ ".xml", 'w') as f:
     f.write(xml_data)
os.chdir(cur_dir)
print(os.getcwd())