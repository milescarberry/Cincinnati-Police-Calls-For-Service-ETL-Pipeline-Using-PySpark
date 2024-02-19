import numpy as np

import pandas as pd 


import json 


import pickle 


from pprint import pprint


import os 


directory = "./data/cin_crime_data/"


directory_files = os.listdir(directory)


json_files = [file for file in directory_files if ".crc" not in file.lower().strip() and "success" not in file.lower().strip()]



for file in json_files:


	with open(directory + file, 'r', errors='ignore') as json_file:

		print(f"\n\n{file}\n\n")

		json_text = json_file.read()

		json_text = json_text.replace("\n", "")

		json_text = json_text.replace("}{", "}, \n{")

		json_text = json_text.split(", \n")

		json_text = [json.loads(j) for j in json_text]



		with open("./data/" + file, 'w') as jfile:


			json.dump(json_text, jfile, indent = 6)







pprint(pd.read_json("./data/" + json_files[1]).head(5))
