import requests
from io import StringIO
import pandas as pd
# import pyspark
# from pyspark.sql import SparkSession
#
#
#
# spark=SparkSession.builder\
#     .appName("api")\
#     .getOrCreate()
# pd.set_option('display.max_columns', None)

import csv
github_api_url = "https://api.github.com/repos/squareshift/stock_analysis/contents/"
response = requests.get(github_api_url)
# print(response)
files = response.json()
# print(files)
csv_files = [file['download_url'] for file in files if file['name'].endswith('.csv')]
# print(csv_files)
a=csv_files[0]
# file_name = a.split("/")[-1].replace(".csv", "")
# print(a)
# print(file_name)
# print(csv_files)
csv_file = csv_files.pop()
# print(csv_file)
d = pd.read_csv(csv_file)
# print(d)
# print(d.columns)
# print(d["Sector"])
dataframes=[]
file_names=[]
for url in csv_files:
    file_name = url.split("/")[-1].replace(".csv", "")
      # print(file_name)
    df = pd.read_csv(url)
    df['Symbol'] = file_name
    dataframes.append(df)
    file_names.append(file_name)
# print(dataframes)
# print(file_names)
combined_df = pd.concat(dataframes, ignore_index=True)
# print(combined_df)
# # print(combined_df.columns)
o_df = pd.merge(combined_df,d,on='Symbol',how='left')
print(o_df)
# b=spark.createDataFrame(o_df)
# b.show()

# # print(o_df.columns)
# print(o_df[2:5])
result = o_df.groupby("Sector").agg({'open':'mean','close':'mean','high':'max','low':'min','volume':'mean'}).reset_index()
# print(result)
#(o_df["timestamp"])
# o_df["timestamp"] = pd.to_datetime(o_df["timestamp"])
# print(o_df["timestamp"])
# filtered_df = o_df[(o_df['timestamp'] >= "2021-01-01") & (o_df['timestamp'] <= "2021-05-26")]
# # print(filtered_df)
# result_time = filtered_df.groupby("Sector").agg({'open':'mean','close':'mean','high':'max','low':'min','volume':'mean'}).reset_index()
# list_sector = ["TECHNOLOGY","FINANCE"]
# result_time = result_time[result_time["Sector"].isin(list_sector)].reset_index(drop=True)
# # print(result_time)
# path=r"D:\Data/stock_data.csv"
# result_time.to_csv(path,header=True)
# print("data has been written successfully")
































