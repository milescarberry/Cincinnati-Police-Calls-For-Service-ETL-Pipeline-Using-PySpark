
# Full code for your reference: We will use this block for setting up the environment in our future videos

# Install java 8

# !apt-get update

# !apt-get install openjdk-8-jdk-headless -qq > /dev/null

# # Download Apache Spark binary: This link can change based on the version. Update this link with the latest version before using

# !wget -q https://downloads.apache.org/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz

# # Unzip file

# !tar -xvzf spark-3.5.0-bin-hadoop3.tgz

# # Install findspark: Adds Pyspark to sys.path at runtime

# !pip install -q findspark

# # Install pyspark

# !pip install pyspark

# !pip install pymongo

# Add environmental variables

# os.environ["JAVA_HOME"] = os.getenv("JAVA_H")

# os.environ["SPARK_HOME"] = os.getenv("SPARK_H")

# findspark will locate spark in the system

import findspark

findspark.init()

import numpy as np

import pandas as pd

import datetime as dt

import requests

import pyspark

import pymongo

import pickle

import time as tm

from pprint import pprint

import os

import sys

import json

import chardet

import warnings

warnings.filterwarnings("ignore", category=DeprecationWarning)

warnings.filterwarnings("ignore", category=FutureWarning)



# os.environ['PYSPARK_PYTHON'] = python


# os.environ['PYSPARK_PYTHON'] = sys.executable

# os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable



from dotenv import load_dotenv

load_dotenv(".env")

import os


# Let's create a SparkSession instance


print("\n\n\nCreating SparkSession Instance\n\n\n")


from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("cin_calls") \
    .config("spark.executer.memory", '44g') \
    .config("spark.executor.memoryOverhead", "44g") \
    .config("spark.driver.memory", '44g') \
    .config("spark.mongodb.input.uri", os.getenv("mongodb_url")) \
    .config("spark.mongodb.output.uri", os.getenv("mongodb_url")) \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector:10.0.2') \
    .getOrCreate()

sparkContext=spark.sparkContext

print("\n\n\nSparkSession Created\n\n\n")

# Script 1:


# def get_data():


#     url = os.getenv('url')


#     start = dt.datetime.strftime(dt.datetime(2019, 1, 1, 0, 0, 0), "%Y-%m-%d").replace("'", "")


#     now = dt.datetime.strftime(dt.datetime.now(), "%Y-%m-%d").replace("'", "")


#     query = f"$where=create_time_incident>='2019-01-01T00:00:00.000' and create_time_incident<='{now}T23:59:59.000'&$limit=1000000000000"


#     # query = "$limit=5"


#     user_agent = os.getenv('user_agent')


#     headers = {


#         "User-Agent": user_agent


#      }


#     response = requests.get(url, query, headers = headers)


#     jsn = response.json()


#     # df = pd.DataFrame(jsn)


#     # df.to_csv("./data/cin_crime_data.csv", index = False)


#     return jsn



# from functools import reduce

# from pyspark.sql import DataFrame



def get_data():


    url = os.getenv('url')


    start = dt.datetime.strftime(dt.datetime(2019, 1, 1, 0, 0, 0), "%Y-%m-%d").replace("'", "")


    now = dt.datetime.strftime(dt.datetime.now(), "%Y-%m-%d").replace("'", "")


    rge = pd.date_range(start = start, end = now, freq = '1M')


    rge  = [dt.datetime.strftime(i, "%Y-%m-%d").replace("'", "") for i in rge]


    rge[0] = start


    rge[-1] = now


    datepairs = [[rge[i], rge[i + 1]] if i != len(rge) - 1 else [rge[i], now] for i in range(len(rge))]


    if datepairs[-2][1] == datepairs[-1][0]:


      datepairs = datepairs[:-1:1]



    for i in range(len(datepairs)):


      if i != 0:

        datepairs[i][0] = dt.datetime.strftime(

            dt.datetime.strptime(datepairs[i][0],

                                 "%Y-%m-%d"

                                 ) + dt.timedelta(days = 1),

            "%Y-%m-%d"

            ).replace("'", "")



    queries = [f"$where=create_time_incident>='{pair[0]}T00:00:00.000' and create_time_incident<='{pair[1]}T23:59:59.000'&$limit=1000000000000" for pair in datepairs]



    # query = f"$where=create_time_incident>='2019-01-01T00:00:00.000' and create_time_incident<='{now}T23:59:59.000'&$limit=1000000000000"

    # query = "$limit=5"


    # response = requests.get(url, query, headers = headers)


    # jsn = response.json()


    # # df = pd.DataFrame(jsn)


    # # df.to_csv("./data/cin_crime_data.csv", index = False)



    user_agent = os.getenv('user_agent')


    headers = {


        "User-Agent": user_agent


    }


    jsons = []



    for query in queries:


      response = requests.get(url, query, headers = headers)


      json = response.json()


      jsons.extend(json)


      print(f"\n\n{len(jsons)}\n\n")



    # with open("./data/pickle/cin_json.pickle", 'wb') as pickle_file:


    #     pickle.dump(jsons, pickle_file, protocol = pickle.HIGHEST_PROTOCOL)



    return spark.createDataFrame(jsons)



    # return reduce(DataFrame.union, dfs)


"""##### In the cloud you can create multiple ChildSession instances."""

# Let's read our .csv file

# df = spark.read.csv(

#     "/content/drive/MyDrive/Projects/cin_crime_data_2019_2024/data/cin_crime_data.csv",

#     header = True,

#     inferSchema = True

# )

print("\n\n\nExtracting Data & Creating Spark DataFrame Object\n\n\n")


df = get_data()


print("\n\n\nSuccessfully Created Spark DataFrame\n\n\n")


# print(df.count(), len(df.columns))

# df.printSchema()

# df.describe()

# df.show(2)

# df.select(['longitude_x', 'latitude_x']).describe().show()

# Show count of nan values by column

# def show_nan(df, nan_criteria = 'nan'):


#     ### Get count of both null and missing values in pyspark


#     from pyspark.sql.functions import isnan, when, count, col


#     # nan_df for non timestamp columns


#     nan_df = df.select([(count(when(isnan(c) | col(c).isNull() | col(c).contains("N/A"), c)) / df.count()).alias(c) for c in [i for i in df.columns] if 'time' not in str(df.select([c])).lower()])


#     # nan_df for timestamp columns (counting only null values)


#     t_nan_df = df.select([(count(when(col(c).isNull(), c)) / df.count()).alias(c) for c in df.columns if 'time' in str(df.select([c])).lower()])



#     # c_nan_df = df.select([(count(when(col(c).contains("N/A") | isnan(c) | col(c).isNull(), c)) / df.count()).alias(c) for c in [i for i in df.columns if 'neighborhood' in i.lower()]])



#     # Convert from pyspark dataframe to pandas dataframe


#     spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")


#     nan_df = nan_df.toPandas()


#     t_nan_df = t_nan_df.toPandas()


#     # c_nan_df = c_nan_df.toPandas()


#     nan_df = pd.concat([nan_df, t_nan_df], axis = 1)


#     nan_df = nan_df.T.reset_index()


#     nan_df.columns = ['column', 'nan_%']


#     if 'nan' not in str(nan_criteria).lower():


#         nan_df = nan_df[nan_df['nan_%'] >= nan_criteria]



#     fig = exp.bar(nan_df, x = 'nan_%', y = 'column')



#     fig.update_layout(bargap = 0.32)



#     fig.update_layout(height = 900)



#     fig.show()



#     return None


# show_nan(df)


from pyspark.sql.functions import year, month, dayofmonth, hour, minute, col


df_ = df.withColumn('create_time_incident_year', year(col('create_time_incident'))) \
                     .withColumn('create_time_incident_month', month(col('create_time_incident'))) \
                     .withColumn('create_time_incident_day', dayofmonth(col('create_time_incident'))) \
                     .withColumn('create_time_incident_hour', hour(col('create_time_incident'))) \
                     .withColumn('create_time_incident_minute', minute(col('create_time_incident'))) \
                     .withColumn('closed_time_incident_month', month(col('closed_time_incident'))) \
                     .withColumn('closed_time_incident_day', dayofmonth(col('closed_time_incident'))) \
                     .withColumn('closed_time_incident_hour', hour(col('closed_time_incident'))) \
                     .withColumn('closed_time_incident_minute', minute(col('closed_time_incident')))


df_ = df_.withColumn('arrival_time_primary_unit_year', year(col('arrival_time_primary_unit'))) \
                     .withColumn('arrival_time_primary_unit_month', month(col('arrival_time_primary_unit'))) \
                     .withColumn('arrival_time_primary_unit_day', dayofmonth(col('arrival_time_primary_unit'))) \
                     .withColumn('arrival_time_primary_unit_hour', hour(col('arrival_time_primary_unit'))) \
                     .withColumn('arrival_time_primary_unit_minute', minute(col('arrival_time_primary_unit')))


df_ = df_.withColumn('dispatch_time_primary_unit_year', year(col('dispatch_time_primary_unit'))) \
                     .withColumn('dispatch_time_primary_unit_month', month(col('dispatch_time_primary_unit'))) \
                     .withColumn('dispatch_time_primary_unit_day', dayofmonth(col('dispatch_time_primary_unit'))) \
                     .withColumn('dispatch_time_primary_unit_hour', hour(col('dispatch_time_primary_unit'))) \
                     .withColumn('dispatch_time_primary_unit_minute', minute(col('dispatch_time_primary_unit')))


# df_.show(2)

# df_.columns

from pyspark.sql.functions import col, lit, concat


df_ = df_.withColumn('coordinates',concat(col('longitude_x').cast("string"), lit(", "), col('latitude_x').cast("string")))

df_ = df_.withColumn(
    "create_closed_timedelta",
    (col("closed_time_incident").cast("timestamp").cast("long") - col("create_time_incident").cast("timestamp").cast("long")) /60
)



df_ = df_.withColumn(
    "dispatch_arrival_timedelta",
    (col("arrival_time_primary_unit").cast("timestamp").cast("long") - col("dispatch_time_primary_unit").cast("timestamp").cast("long")) / 60
)


df_ = df_.withColumn(

    "create_dispatch_timedelta",

    (col("dispatch_time_primary_unit").cast('timestamp').cast('long') - col("create_time_incident").cast("timestamp").cast("long")) / 60


    )



df_ = df_.withColumn(

    "create_arrival_timedelta",

    (col("arrival_time_primary_unit").cast('timestamp').cast('long') - col("create_time_incident").cast("timestamp").cast("long")) / 60


    )

# df_.columns

from pyspark.sql.window import Window

from pyspark.sql.functions import dense_rank


windowSpec = Window.partitionBy("event_number").orderBy(col("create_time_incident").desc())

df_ranked = df_.withColumn("event_rank", dense_rank().over(windowSpec))

# df_ranked.show()

df_ranked = df_ranked.where((col('event_rank') == 1) & ~(col('district').isNull()))

df_ranked = df_ranked.drop('event_rank')

# show_nan(df_ranked)

"""### Exporting PySpark DataFrame"""

# df_ranked.write.mode("overwrite").parquet("/content/drive/MyDrive/Projects/cin_crime_data_2019_2024/data/cin_crime_data.parquet")


# Script 2:



from pyspark.sql.functions import avg, min, max, count, col, isnan, round, lit, concat


# df_ranked.columns

# df_ranked.show(2)

agg_df = df_ranked.groupby(


      [

       'create_time_incident_year',

       'create_time_incident_month',

       'address_x',

       'disposition_text',

       'incident_type_id',

      #  'incident_type_desc',

       'priority',

      #  'priority_color',

       'beat',

       'district',

       'cpd_neighborhood',

       'community_council_neighborhood',

       'coordinates'

      ]).agg(

            count(col('event_number')).alias('num_incidents'),

            avg(col('create_closed_timedelta')).alias('avg_create_closed_timedelta'),

            avg(col('dispatch_arrival_timedelta')).alias('avg_dispatch_arrival_timedelta'),

            avg(col('create_dispatch_timedelta')).alias('avg_create_dispatch_timedelta'),

            avg(col('create_arrival_timedelta')).alias('avg_create_arrival_timedelta')

          ).sort(      [

       'create_time_incident_year',

       'create_time_incident_month',

       'address_x',

       'disposition_text',

       'incident_type_id',

      #  'incident_type_desc',

       'priority',

      #  'priority_color',

       'beat',

       'district',

       'cpd_neighborhood',

       'community_council_neighborhood',

       'coordinates'

      ], ascending = True)


# agg_df.show(2)

agg_df = agg_df.withColumn(


      "create_time_incident_year_month",


      concat(col('create_time_incident_year').cast('string'), lit("-"), col('create_time_incident_month').cast('string'))


)


agg_df = agg_df.drop('create_time_incident_year', 'create_time_incident_month')


# agg_df.show(2)

cols = [agg_df.columns[-1]]

cols.extend(agg_df.columns[:-1:1])

agg_df = agg_df.select(cols)

# agg_df.show(2)

categorical_cols = ['create_time_incident_year_month',
 'address_x',
 'disposition_text',
 'incident_type_id',
 'priority',
 'beat',
 'district',
 'cpd_neighborhood',
 'community_council_neighborhood',
 'coordinates']


distinct_cats = {k: v for k, v in zip(categorical_cols, [agg_df.select([col]).distinct() for col in categorical_cols])}

from pyspark.sql.window import Window

from pyspark.sql.functions import col, row_number


for c in categorical_cols:

  windowSpec = Window.partitionBy().orderBy(col(c).desc())

  dframe = distinct_cats[c].withColumn("id", row_number().over(windowSpec))

  dframe = dframe.select(['id', c])

  distinct_cats[c] = dframe

# distinct_cats['incident_type_id'].where(col('incident_type_id').isNull()).show()

# cats_dict = {}

# for c in categorical_cols:

#   cats_dict[c] = {k: v for k, v in zip(distinct_cats[c].select([c]).toPandas()[c].values, [(float(i),) for i in distinct_cats[c].select(['id']).toPandas()['id'].values])}

# rdd_ = agg_df.rdd.map(

#     lambda x: (

#           x.create_time_incident_year_month,


#           cats_dict['address_x'][x.address_x],


#           cats_dict['disposition_text'][x.disposition_text],


#           cats_dict['incident_type_id'][x.incident_type_id],


#           cats_dict['priority'][x.priority],


#           cats_dict['beat'][x.beat],


#           cats_dict['district'][x.district],


#           cats_dict['cpd_neighborhood'][x.cpd_neighborhood],


#           cats_dict['community_council_neighborhood'][x.community_council_neighborhood],


#           cats_dict['coordinates'][x.coordinates],


#           x.num_incidents,


#           x.avg_create_closed_timedelta,


#           x.avg_dispatch_arrival_timedelta,


#           x.avg_create_dispatch_timedelta,


#           x.avg_create_arrival_timedelta





#         )


#     )


# agg_df_2 = rdd_.toDF(agg_df.columns)

# from pyspark.sql.functions import udf, col


# from pyspark.sql.types import StringType, IntegerType



# id_cols = ['address_x',
#  'disposition_text',
#  'incident_type_id',
#  'priority',
#  'beat',
#  'district',
#  'cpd_neighborhood',
#  'community_council_neighborhood',
#  'coordinates']


# timedelta_cols = [i for i in agg_df_2.columns if 'timedelta' in i.lower().strip()]



# # Let's define our user defined function


# def get_first_element(tuple_value):


#   return int(tuple_value[0]) if tuple_value else None



# get_first_element_udf = udf(get_first_element, IntegerType())            # Please adjust return type if needed.



# for c in id_cols:


#   agg_df_2 = agg_df_2.withColumn(c, get_first_element_udf(col(c)))



# for c in timedelta_cols:


#   agg_df_2 = agg_df_2.withColumn(c, round(col(c), 1))

# agg_df_2.show(2)

# agg_df_2 = agg_df_2.select([col('create_time_incident_year_month'),
#  col('address_x').alias("address_x_id"),
#  col('disposition_text').alias("disposition_text_id"),
#  col('incident_type_id').alias("incident_type_id_id"),
#  col('priority').alias("priority_id"),
#  col('beat').alias("beat_id"),
#  col('district').alias("district_id"),
#  col('cpd_neighborhood').alias("cpd_neighborhood_id"),
#  col('community_council_neighborhood').alias("community_council_neighborhood_id"),
#  col('coordinates').alias("coordinates_id"),
#  col('num_incidents'),
#  col('avg_create_closed_timedelta'),
#  col('avg_dispatch_arrival_timedelta'),
#  col('avg_create_dispatch_timedelta'),
#  col('avg_create_arrival_timedelta')]
# )

# agg_df.show(7)

timedelta_cols = [c for c in agg_df.columns if 'timedelta' in c.lower().strip()]

for t in timedelta_cols:


  agg_df = agg_df.withColumn(t, round(col(t), 1))



print("\n\n Saving DataFrame as Parquet Files\n\n")


# agg_df.write.mode("overwrite").parquet("./data/cin_crime_data.parquet")



agg_df.write.mode("overwrite").json("./data/cin_crime_data")



print("\n\nSaved DataFrame as Parquet Files\n\n")



# Script 3


dataset_link = "./data/cin_crime_data/"


files = os.listdir(dataset_link)


filenames = [i for i in files if ".crc" not in i.lower().strip() and "success" not in i.lower().strip()]


filenames.sort()



for file in filenames:


    with open(dataset_link + file, 'r', errors='ignore') as json_file:

        print(f"\n\n{file}\n\n")

        json_text = json_file.read()

        json_text = json_text.replace("\n", "")

        json_text = json_text.replace("}{", "}, \n{")

        json_text = json_text.split(", \n")

        json_text = [json.loads(j) for j in json_text]



        with open(dataset_link + file, 'w') as jfile:


            json.dump(json_text, jfile, indent = 6)




df = pd.DataFrame()



for name in filenames:


    print(f"\n\n{name}\n\n")


    # with open(dataset_link + name, 'rb') as f:


    #     result = chardet.detect(f.read())


    # _df = pd.read_json(dataset_link + name, encoding = result['encoding'])


    _df = pd.read_json(dataset_link + name)


    _df = _df.reset_index(drop = True)


    df = pd.concat([df, _df], axis = 0)




timedelta_cols = [i for i in df.columns if 'timedelta' in i.lower()]


categorical_cols = [i for i in df.columns if i not in timedelta_cols and i.lower().strip() != 'num_incidents']


length = len(df)


indices = [i for i in range(0,length,50000) if i < length]


tuples = []


for i in range(len(indices)):

  if i != len(indices) - 1:

    tuples.append((indices[i], indices[i+1]))


  else:

    tuples.append((indices[i],))



df_dict = {}


cat_col_vals_dict = {k: v for k, v in zip(categorical_cols, [[] for i in categorical_cols])}



for i in range(len(tuples)):


  if i != len(tuples) - 1:


    _ = df.iloc[tuples[i][0]:tuples[i][1]:1, ::1].reset_index(drop = True)



    # _.create_time_incident_year_month = _.create_time_incident_year_month.apply(

    #     lambda x: dt.datetime.strptime(x, "%Y-%m")

    #     )


    for j in categorical_cols:


      cat_col_vals_dict[j].extend(list(_[j].unique()))



    df_dict[f"chunk_{i}"] = _


  else:


    _ = df.iloc[tuples[i][0]::1, ::1].reset_index(drop = True)



    # _.create_time_incident_year_month = _.create_time_incident_year_month.apply(

    #     lambda x: dt.datetime.strptime(x, "%Y-%m")

    #     )




    for j in categorical_cols:


      cat_col_vals_dict[j].extend(list(_[j].unique()))



    df_dict[f"chunk_{i}"] = _




for i in categorical_cols:


  cat_col_vals_dict[i] = list(set(cat_col_vals_dict[i]))


  _dict = {k: v for k, v in zip([j for j in range(len(cat_col_vals_dict[i]))], cat_col_vals_dict[i])}


  __ = pd.DataFrame()


  __['id'] = list(_dict.keys())


  __[i] = list(_dict.values())


  cat_col_vals_dict[i] = __


def open_connection():


  connection_string = os.getenv('mongodb_url')


  client = None



  try:


    client = pymongo.MongoClient(connection_string)

    client.admin.command('ping')

    print("Pinged your deployment. You successfully connected to MongoDB!")



  except BaseException as e:

    print(e)

    client = None


  return client





def db_insert(data, dbname, collname):


  counter = 0


  while True:


    try:

      client = open_connection()


      if client:


        db = client[dbname]


        coll = db[collname]


        if collname in db.list_collection_names():

          c = db[collname]

          if c == None:

            c.drop()


        print("\nPreparing to insert data.\n")


        # Write data to collection


        if len(data) > 1:

          coll.insert_many(data.to_dict(orient = 'records'))


        elif len(data) == 1:


          coll.insert_one(data.to_dict(orient = 'records'))


        else:

          pass



        break        # Break out of the while loop




      else:


        if counter > 5:

          break


        else:

          print("Will retry after 1 minutes.")

          tm.sleep(60)

          counter += 1

          continue


    except BaseException as e:


      print(e)


      if counter > 5:

        break

      else:

        tm.sleep(60)

        print("Will retry after 1 minutes.")

        counter += 1

        continue




def db_insert_mappings(data, dbname, collname):


  counter = 0


  while True:


    try:

      client = open_connection()


      if client:


        db = client[dbname]


        coll = db[collname]


        if collname in db.list_collection_names():

          c = db[collname]

          if c == None:

            c.drop()


        print("\nPreparing to insert data.\n")


        # Write data to collection


        coll.insert_one(data)


        # if len(data) > 1:

        #   coll.insert_many(data.to_dict(orient = 'records'))


        # elif len(data) == 1:


        #   coll.insert_one(data.to_dict(orient = 'records'))


        # else:

        #   pass



        break        # Break out of the while loop




      else:


        if counter > 5:

          break


        else:

          print("Will retry after 1 minutes.")

          tm.sleep(60)

          counter += 1

          continue


    except BaseException as e:


      print(e)


      if counter > 5:

        break

      else:

        tm.sleep(60)

        print("Will retry after 1 minutes.")

        counter += 1

        continue



# def insert_mappings(dbname):


#   collnames = list(distinct_cats.keys())

#   datasets = list(distinct_cats.values())


#   for i in range(len(collnames)):


#     db_insert(data = datasets[i], dbname = dbname, collname = collnames[i])



def db_get(dbname, collname):

  counter = 0

  while True:

    try:

      client = open_connection()

      if client:

        db = client[dbname]

        coll = db[collname]


        if coll.count_documents({}) > 0:


          return coll.find({}, projection = {"_id": False})


      else:

        if counter > 5:

          break


        else:

          print("Retrying after 1 minutes.")

          tm.sleep(60)

          counter += 1

          continue


    except BaseException as e:


      print(e)

      if counter > 5:

        break


      else:

        print("Retrying after 1 minutes.")

        tm.sleep(60)

        counter += 1

        continue




def drop_collections(dbname):


  counter = 0


  while True:


    mclient = open_connection()


    try:

      if mclient:

        db = mclient[dbname]

        for c in db.list_collection_names():

          coll = db[c]

          if coll == None:

            coll.drop()


        print("Dropped all collections.")


        break


      else:

        if counter > 5:

          break

        else:

          counter += 1

          tm.sleep(60)

          continue


    except BaseException as e:


      print(e)


      if counter > 5:

        break


      else:

        counter += 1

        tm.sleep(60)

        continue

drop_collections(os.getenv('mongo_db_mappings_dbname'))

drop_collections(os.getenv('mongodb_dbname'))

dframes = list(df_dict.items())

for dframe in dframes:

  db_insert(dframe[1], os.getenv('mongodb_dbname'), dframe[0])


for i in categorical_cols:

  db_insert(cat_col_vals_dict[i], os.getenv('mongo_db_mappings_dbname'), i)



print("\n\nData Insertion Done Successfully!\n\n")