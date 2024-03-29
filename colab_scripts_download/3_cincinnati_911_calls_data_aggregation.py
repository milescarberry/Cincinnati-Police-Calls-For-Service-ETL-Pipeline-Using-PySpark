# -*- coding: utf-8 -*-
"""3_cincinnati_pyspark_practice.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/17clq1IulMImu3uzvUJSyYQA89_X4-dnr
"""

from google.colab import drive

# Mounting Google Drive

drive.mount('/content/drive')

# Full code for your reference: We will use this block for setting up the environment in our future videos

# Install java 8

!apt-get update

!apt-get install openjdk-8-jdk-headless -qq > /dev/null

# Download Apache Spark binary: This link can change based on the version. Update this link with the latest version before using

!wget -q https://downloads.apache.org/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz

# Unzip file

!tar -xvzf spark-3.5.0-bin-hadoop3.tgz

# Install findspark: Adds Pyspark to sys.path at runtime

!pip install -q findspark

# Install pyspark

!pip install pyspark

!pip install pymongo

# Add environmental variables

import os

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"

os.environ["SPARK_HOME"] = "/content/spark-3.5.0-bin-hadoop3"

# findspark will locate spark in the system

import findspark

findspark.init()

import numpy as np

import pandas as pd

import matplotlib.pyplot as plt

import seaborn as sns

sns.set_context('paper', font_scale = 1.4)

from plotly import express as exp, graph_objects as go, io as pio

pio.templates.default = 'ggplot2'

from plotly.subplots import make_subplots

import pickle

import datetime as dt

import requests

import pyspark

from time import sleep

from google.colab import userdata

from pprint import pprint

import os

import sys

import json

import warnings

warnings.filterwarnings("ignore", category=DeprecationWarning)

warnings.filterwarnings("ignore", category=FutureWarning)

os.environ['PYSPARK_PYTHON'] = sys.executable

os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Let's create a SparkSession instance

from pyspark.sql import SparkSession

# spark = SparkSession.builder.appName("Analyze").config("spark.executor.memoryOverhead", "1g").getOrCreate()

# .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector:10.0.2') \


spark = SparkSession \
    .builder \
    .appName("Analyze") \
    .config("spark.executer.memory", '44g') \
    .config("spark.executor.memoryOverhead", "44g") \
    .config("spark.driver.memory", '44g') \
    .config("spark.mongodb.input.uri", userdata.get("mongodb_url")) \
    .config("spark.mongodb.output.uri", userdata.get("mongodb_url")) \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector:10.0.2') \
    .getOrCreate()

spark

df_ = spark.read.parquet(

    "/content/drive/MyDrive/Projects/cin_crime_data_2019_2024/data/cin_crime_data.parquet",

    inferSchema = True


  )


df_.count(), len(df_.columns)


from pyspark.sql.functions import avg, min, max, count, col, isnan, round, lit, concat


def analyze_col_is_null(column = '', sec_column = '', sec_column_2 = ''):



	df_nan = df_.where(isnan(col(column)) | col(column).isNull() | col(column).contains("N/A"))


	# df_nan.show(3)


	print(df_nan.count())


	# Year-wise percent distribution of NaN values


	df_nan_by_yr = df_nan.groupby(

	     ['create_time_incident_year']

	     ).agg(

	         round((count('event_number') / df_nan.count()) * 100, 2).alias('%')

	         ).sort(

	              ['create_time_incident_year'],

	              ascending = True


	)


	print(df_nan_by_yr.count())



	# Generate Pivot Table



	years = df_.select(['create_time_incident_year']).distinct().sort(['create_time_incident_year'], ascending = [True]).toPandas().iloc[::1, 0].values.tolist()



	# grp_df = df_nan.groupBy(col(sec_column), col(sec_column_2), col('create_time_incident_year')).agg(

	#     round(count(col('event_number')) / df_nan.count(), 3).alias('%')).sort(


	#          [sec_column, sec_column_2, 'create_time_incident_year'], ascending = [True, True, True]


	#          )


	if len(sec_column_2) > 1:


		pivot_grp_df = df_nan.groupBy(


		     [sec_column, sec_column_2]


		     ).pivot(

		         'create_time_incident_year'

		         , years

		         ).agg(


		              count(

		              "event_number"


		              )


		             ).sort(

		                    [sec_column, sec_column_2]


		                    , ascending = [True, True]


		                  )




		print(pivot_grp_df.count())



		df_nan_by_yr.write.parquet(f"/content/drive/MyDrive/Projects/cin_crime_data_2019_2024/saved_data/{column}_{sec_column}_{sec_column_2}_year_wise_nan_analysis.parquet")



		pivot_grp_df.write.parquet(f"/content/drive/MyDrive/Projects/cin_crime_data_2019_2024/saved_data/{column}_{sec_column}_{sec_column_2}_year_wise_pivot_analysis.parquet")





	else:


		pivot_grp_df = df_nan.groupBy(


	    	[sec_column]


	    ).pivot(

	         'create_time_incident_year'

	         , years

	         ).agg(


	              count(

	              "event_number"


	              )


	             ).sort(

	                    [sec_column]


	                    , ascending = [True]


	                  )





		print(pivot_grp_df.count())



		df_nan_by_yr.write.mode("overwrite").parquet(f"/content/drive/MyDrive/Projects/cin_crime_data_2019_2024/saved_data/{column}_{sec_column}_year_wise_nan_analysis.parquet")



		pivot_grp_df.write.mode("overwrite").parquet(f"/content/drive/MyDrive/Projects/cin_crime_data_2019_2024/saved_data/{column}_{sec_column}_year_wise_pivot_analysis.parquet")




	return None

# Call 'analyze_coll_is_null' function

# analyze_col_is_null("cpd_neighborhood", 'create_time_incident_day', '')

df_.columns

df_.show(2)

agg_df = df_.groupby(


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


agg_df.show(2)

agg_df = agg_df.withColumn(


      "create_time_incident_year_month",


      concat(col('create_time_incident_year').cast('string'), lit("-"), col('create_time_incident_month').cast('string'))


)


agg_df = agg_df.drop('create_time_incident_year', 'create_time_incident_month')


agg_df.show(2)

cols = [agg_df.columns[-1]]

cols.extend(agg_df.columns[:-1:1])

agg_df = agg_df.select(cols)

agg_df.show(2)

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

agg_df.show(7)

timedelta_cols = [c for c in agg_df.columns if 'timedelta' in c.lower().strip()]

for t in timedelta_cols:


  agg_df = agg_df.withColumn(t, round(col(t), 1))

agg_df.write.mode("overwrite").parquet("/content/drive/MyDrive/Projects/cin_crime_data_2019_2024/data/cin_crime_data.parquet")

import pymongo



def open_connection():


  connection_string = userdata.get('mongodb_url')


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


        print("\nPreparing to insert data.\n")


        # Write data to collection


        # data.write.format(

        #     "mongodb"


        #     ).mode(

        #         "append"

        #         ).option(

        #             "database",

        #             dbname

        #             ).option(

        #                 "collection",

        #                 collname

        #                 ).save()


        # data.write.option(

        #     "database",

        #     dbname).option(

        #         "collection",

        #         collname).format(

        #             "mongodb"

        #             ).save(

        #                 userdata.get("mongodb_url")


        #                 )


        for c in data.columns:

          data = data.withColumn(c, col(c).cast('string'))


        for row in data.collect():

          row_dict = row.asDict()

          coll.insert_one(row_dict)



        break        # Break out of the while loop




      else:


        if counter > 5:

          break


        else:

          print("Will retry after 1 minutes.")

          sleep(60)

          counter += 1

          continue


    except BaseException as e:


      print(e)


      if counter > 5:

        break

      else:

        sleep(60)

        print("Will retry after 1 minutes.")

        counter += 1

        continue




def insert_mappings(dbname):


  collnames = list(distinct_cats.keys())

  datasets = list(distinct_cats.values())


  for i in range(len(collnames)):


    db_insert(data = datasets[i], dbname = dbname, collname = collnames[i])



def db_get(dbname, collname):

  counter = 0

  while True:

    try:

      client = open_connection()

      if client:

        db = client[dbname]

        coll = db[collname]


        if coll.count_documents({}) > 0:


          return spark.read.format(

              "mongodb"

              ).mode(

                  "append"

                  ).option(

                      "database",

                      dbname

                      ).option(

                          "collection",

                          collname

                          ).load()




      else:

        if counter > 5:

          break


        else:

          print("Retrying after 1 minutes.")

          sleep(60)

          counter += 1

          continue


    except BaseException as e:


      print(e)

      if counter > 5:

        break


      else:

        print("Retrying after 1 minutes.")

        sleep(60)

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

          db.drop(c)


        print("Dropped all collections.")


        break


      else:

        if counter > 5:

          break

        else:

          counter += 1

          sleep(60)

          continue


    except BaseException as e:


      print(e)


      if counter > 5:

        break


      else:

        counter += 1

        sleep(60)

        continue

# drop_collections(dbname = userdata.get('mongodb_dbname'))

# drop_collections(dbname = userdata.get('mongo_db_mappings_dbname'))

# db_insert(data = agg_df, dbname = userdata.get('mongodb_dbname'), collname = userdata.get('mongodb_collname'))

# insert_mappings(dbname = userdata.get('mongo_db_mappings_dbname'))

# # Convert to JSON-formatted RDD
# json_rdd = agg_df.toJSON()

# # Access data as JSON string

# json_string = json_rdd.collect()[0]


# print(json_string)



# def process_chunk(partition, chunk_size):
#     iterator = partition.toLocalIterator()
#     while True:
#         chunk = [next(iterator) for _ in range(chunk_size)]
#         if not chunk:
#             break
#         # Process your chunk of data here

#         print(chunk)


# # Process data in chunks of 10

# agg_df.rdd.foreachPartition(lambda partition: process_chunk(partition, 10))





# # client = open_connection()


# # if client:

# #         db = client[userdata.get('mongodb_dbname')]

# #         coll = db[userdata.get('mongodb_collname')]


# #         print("\nPreparing to insert data.\n")


# #         for row in agg_df.collect():

# #           row_dict = row.asDict()

# #           coll.insert_one(row_dict)


# #         # Write data to collection


# #         # for c in agg_df.columns:

# #         #   agg_df = agg_df.withColumn(c, col(c).cast('string'))

# #         # agg_df.write.format(

# #         #     "mongodb"


# #         #     ).mode(

# #         #         "append"

# #         #         ).option(

# #         #             "database",

# #         #             userdata.get('mongodb_dbname')

# #         #             ).option(

# #         #                 "collection",

# #         #                 userdata.get('mongodb_collname')

# #         #                 ).save()


# #         # data.write.option(

# #         #     "database",

# #         #     dbname).option(

# #         #         "collection",

# #         #         collname).format(

# #         #             "mongodb"

# #         #             ).save(

# #         #                 userdata.get("mongodb_url")


# #         #                 )






# # else:


# #   print("No Client")

# from sqlalchemy import create_engine, text


# # Creating Database Schema


# with open(

#     "/content/drive/MyDrive/Projects/cin_crime_data_2019_2024/cincinnati_police_calls_for_service_db_ddl.sql",

#     'r'

#     ) as ddl_text:


#   ddl = ddl_text.read()


#   ddl = ddl.replace('\n', '')


#   ddl = ddl.split(";")



# def create_sqlalchemy_engine():


#     engine = ' '


#     try:

#       engine = create_engine(userdata.get("mysql_connection_link"))


#       return engine


#     except:


#       print("Failed to create sqlalchemy engine.")


#       engine = None


#       return engine


# def create_db_schema():


#   counter = 0


#   while True:


#     try:

#       engine = create_sqlalchemy_engine()


#       if engine:


#         with engine.connect() as conn:


#             for command in ddl:


#               if len(command) > 1:


#                 result = conn.execute(text(command))


#                 conn.commit()


#                 print(result)


#                 print()




#         break


#       else:


#         if counter > 5:

#           break


#         else:

#           counter += 1

#           continue




#     except BaseException as e:


#       print(e)


#       if counter <= 5:

#         counter += 1

#         continue

#       else:

#         break



# create_db_schema()

# # Feeding Data Into Schema Tables


# # Function to write data using SQLAlchemy


# # Prepare sql insert statement with placeholders


# columns = list(distinct_cats.keys())


# for i in range(len(distinct_cats.keys())):

#   if i == 0:

#     continue

#   table_name = columns[i].replace("'", "")


#   column_names =  str(tuple(distinct_cats[columns[i]].columns)).replace("'", "")


#   placeholders = str(tuple(['%s' for i in range(len(distinct_cats[columns[i]].columns))])).replace("'", '')


#   insert_stmt = f"insert into {table_name} {column_names} values {placeholders}"


#   print()


#   print(insert_stmt)


#   print()


#   def write_to_mysql_table(partition):

#       counter = 0

#       while True:

#         engine = create_sqlalchemy_engine()

#         if engine:

#           print(engine)

#           with engine.connect() as conn:

#               print(conn)

#               for row in partition.collect():

#                   # Prepare insert statement with placeholders

#                   # insert_stmt = "INSERT INTO your_table_name (column1, column2, ...) VALUES (%s, %s, ...)"

#                   # Format values according to data types

#                   formatted_values = [row[col] for col in row.schema.names]

#                   print(formatted_values)

#                   # Execute the insert statement with values

#                   conn.execute(insert_stmt, formatted_values)

#                   conn.commit()

#               break

#         else:

#           if counter > 5:

#             break

#           else:

#             counter += 1

#             continue


#   # Write data using foreachPartition

#   distinct_cats[columns[i]].foreachPartition(write_to_mysql_table)