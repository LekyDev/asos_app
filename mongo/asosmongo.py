#!/usr/bin/python3
import datetime
from pyspark.sql import SparkSession

# load mongo data
input_uri = "mongodb://127.0.0.1/tweets.cluster"
output_uri = "mongodb://127.0.0.1/tweets.cluster"

my_spark = SparkSession\
    .builder\
    .appName("MyApp")\
    .config("spark.mongodb.input.uri", input_uri)\
    .config("spark.mongodb.output.uri", output_uri)\
    .config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.12:2.4.2')\
    .getOrCreate()

# date range
date_start = str(datetime.datetime.strptime("2020-05-01", '%Y-%m-%d').isoformat()) + "Z"
date_end = str(datetime.datetime.strptime("2020-05-30", '%Y-%m-%d').isoformat()) + "Z"

pipeline = { \
    '$match': { \
        '$and': [ \
             { \
                'created_at_date': { \
                    '$gt': { \
                        '$date': date_start \
                    } \
                } \
             }, \
             { \
                'created_at_date': { \
                    '$lt': { \
                        '$date': date_end \
                    } \
                } \
            }  \
        ], \
        'full_text': { \
            '$regex':'covid', \
            '$options':'i' \
        } \
    } \
}

df = my_spark.read.format('com.mongodb.spark.sql.DefaultSource').option("pipeline", pipeline).load()
# create view
df.createOrReplaceTempView("twitter_user_timeline_guardian")
# query
SQL_QUERY = "SELECT id, created_at, full_text, text FROM twitter_user_timeline_guardian"
new_df = my_spark.sql(SQL_QUERY)
print(new_df.show(df.count(), truncate=False))
