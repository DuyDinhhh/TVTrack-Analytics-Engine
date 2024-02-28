import findspark
findspark.init()
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
import pandas as pd
spark = SparkSession.builder \
.appName("CustomerActiviry") \
.config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
.getOrCreate()
from pyspark.sql.window import Window
import pyspark.sql.functions as sf
from pyspark.sql.functions import concat_ws
import datetime
from pyspark.sql import functions as F
import os

def find_Activeness(df):
    number_of_days = df.select(df.Day).distinct().count()
    df.select(df.Day).distinct().show()
    df = df.groupBy("Contract").agg(F.countDistinct(df.Day).alias("Active_ness"))
    df = df.withColumn("Active_ness",
                       F.when((F.col("Active_ness") >= int(number_of_days/2)), "High")
                       .otherwise("Low"))
    return df

def find_CustomerTaste(df):
    df=df.withColumn("Taste",concat_ws("-",when(df.ChildDuration.isNotNull(),"Child")
                                    ,when(df.MovieDuration.isNotNull(),"Movies")
                                    ,when(df.RelaxDuration.isNotNull(),"Relax")
                                    ,when(df.SportDuration.isNotNull(),"Sport")
                                    ,when(df.TVDuration.isNotNull(),"TV")
                                    ))
    return df
def find_MostWatch(df):
    df=df.withColumn("MostWatch",greatest(df.ChildDuration,df.MovieDuration,df.RelaxDuration,df.SportDuration,df.TVDuration))
    df=df.withColumn("MostWatch",when(df.MostWatch==df.TVDuration,"TV")
                .when(df.MostWatch==df.MovieDuration,"Movies")
                .when(df.MostWatch==df.RelaxDuration,"Relax")
                .when(df.MostWatch==df.SportDuration,"Sport")
                .when(df.MostWatch==df.ChildDuration,"Child"))
    return df

def processing_data(df):
    total_data = df.groupBy("Contract").agg(sum(df.ChildDuration).alias("ChildDuration"),
                                  sum(df.TVDuration).alias("TVDuration"),
                                  sum(df.RelaxDuration).alias("RelaxDuration"),
                                  sum(df.SportDuration).alias("SportDuration"),
                                  sum(df.MovieDuration).alias("MovieDuration"))
    print('------------------------')
    print('Processing active status')
    print('------------------------')
    active_ness = find_Activeness(df)
    total_data = total_data.join(active_ness,"Contract","left")
    print('------------------------')
    print('Processing most watch')
    print('------------------------')
    total_data = find_MostWatch(total_data)
    print('------------------------')
    print('Processing customer taste')
    print('------------------------')
    total_data = find_CustomerTaste(total_data)
    return total_data.fillna(0)

def etl_1_day(path):
    print('------------------------')
    print('Read data from Json file')
    print('------------------------')
    df = spark.read.json(path)
    print('----------------------')
    print('Showing data structure')
    print('----------------------')
    df.printSchema()
    print('------------------------')
    print('Transforming data')
    print('------------------------')
    df=df.select("_source.*")
    df = df.drop("Mac")
    df=df.withColumn("Type",when((col("AppName")=="KPLUS")|(col("AppName")=="CHANNEL")|(col("AppName")=="VOD"),"TVDuration")
                 .when((col("AppName")=="RELAX"),"RelaxDuration")
                 .when((col("AppName")=="FIMS"),"MovieDuration")
                 .when((col("AppName")=="CHILD"),"ChildDuration")
                 .when((col("AppName")=="SPORT"),"SportDuration"))
    df = df.filter((col("Contract")!="0")&(col("Contract")!="113.182.209.48"))
    df=df.groupBy("Contract").pivot("Type").sum("TotalDuration")
    return df

def etl_1_month(path,start_date,end_date):
    # files = os.listdir(path)
    files =generate_date_range(start_date,end_date)
    print('========================')
    print('ETL file '+files[0])
    print('========================')
    df = etl_1_day(path+files[0]+".json")
    df = df.withColumn("Day",lit(convert_to_datevalue(files[0])))
    for file in files[1:]:
        print('========================')
        print('ETL file '+file+".json")
        print('========================')
        df = df.union(etl_1_day(path+file+".json").withColumn("Day",lit(convert_to_datevalue(file))))
    df = processing_data(df)
    return df


def convert_to_datevalue(value):
	date_value = datetime.datetime.strptime(value,"%Y%m%d").date()
	return date_value

def date_range(start_date,end_date):
	date_list = []
	current_date = start_date
	while current_date <= end_date:
		date_list.append(current_date.strftime("%Y%m%d"))
		current_date += datetime.timedelta(days=1)
	return date_list

def generate_date_range(from_date,to_date):
	from_date = convert_to_datevalue(from_date)
	to_date = convert_to_datevalue(to_date)
	date_list = date_range(from_date,to_date)
	return date_list

def import_to_postgresql(result):
    result.write.format("jdbc") \
    .option("driver","org.postgresql.Driver") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "customeractivity") \
    .mode("overwrite") \
    .option("user", "postgres") \
    .option("password", "root") \
    .save()
    return print('Data imported successfully')

def maintask(path,save_path):
    start_date = "20220401"
    end_date = "20220407"
    df=etl_1_month(path,start_date,end_date)
    print('------------------------')
    print('Write to csv file')
    print('------------------------')
    print("Data from "+ str(convert_to_datevalue(start_date))+" to " + str(convert_to_datevalue(end_date))+":")
    # df.repartition(1).write.csv(save_path, mode='overwrite', header=True)
    print('------------------------')
    print('Showing result')
    print('------------------------')
    df.show()
    print('------------------------')
    print('Importing result to postgresql')
    print('------------------------')
    import_to_postgresql(df)
    return print("Task successful!")

save_path="/Users/nguyentadinhduy/Documents/SQL_THLONG/DE_Gen5_Bigdata/CLASS4/CleanData"
path="/Users/nguyentadinhduy/Documents/SQL_THLONG/DE_Gen5_Bigdata/CLASS4/DataRaw/"
maintask(path,save_path)
