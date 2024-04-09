import findspark
findspark.init()

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.context import SparkContext
import pandas as pd
from pyspark.sql.window import Window
import datetime


spark = SparkSession.builder.config("spark.driver.memory", "8g").getOrCreate()

# path = 'D:\\WORKSPACE\\DE\\study_de\\Big_Data\\Items Shared on 4-29-2023\\Dataset\\log_content\\20220401.json'

# df = spark.read.json(path)


def convert_to_datevalue(value):
	date_value = datetime.datetime.strptime(value,"%Y%m%d").date()
	return date_value
	
def date_range(start_date, end_date):
	date_list = []
	current_date = start_date
	while current_date <= end_date:
		date_list.append(current_date.strftime("%Y%m%d"))
		current_date += datetime.timedelta(days=1)
	return date_list
	
def genarate_date_range(from_date, to_date):
	from_date = convert_to_datevalue(from_date)
	to_date = convert_to_datevalue(to_date)	
	date_list = date_range(from_date, to_date)
	return date_list

def get_new_path(path,listDays):
    list_path_new = []
    for days in listDays:
        path_new = path + '\\' + days + '.json'
        list_path_new.append(path_new)	
    return list_path_new
        
def read_data(path):
    print("------------ => 3.1. Read data--------------")
    df = spark.read.json(path)
    return df


def process_category(df):
    data = df.withColumn('Category',
                when(
                     (col('_source.AppName') == 'KPLUS') | 
                     (col('_source.AppName') == 'RELAX'),
                     "Giải trí"
                    )
                .when(
                     (col('_source.AppName') == 'CHILD'),
                     "Trẻ em"
                    )
                .when(
                     (col('_source.AppName') == 'CHANNEL') |
                     (col('_source.AppName') == 'VOD'),
                     "Truyền hình"
                    )
                .when(
                     (col('_source.AppName') == 'FIMS'), 
                     "Phim ảnh"
                )
                .when(
                     (col('_source.AppName') == 'SPORT'),
                     "Thể thao"
                )
                .otherwise("Khác")
            )  
    
    return data

def process_groupby_data(df):
    data = df.groupBy('_source.Contract', 'Category').agg(
        sum('_source.TotalDuration').alias('TotalDuration')
    )   

    return data

def process_pivot_data(df):
    data =  df.groupBy('Contract').pivot('Category')\
        .agg(sum('TotalDuration'))\
        .select(
            'Contract',
            'Giải trí', 'Phim ảnh', 'Trẻ em', 'Thể thao', 'Truyền hình')\
        .withColumnRenamed('Giải trí', 'TVDuration')\
        .withColumnRenamed('Phim ảnh', 'MovieDuration')\
        .withColumnRenamed('Trẻ em', 'ChildDuration')\
        .withColumnRenamed('Thể thao', 'SportDuration')\
        .withColumnRenamed('Truyền hình', 'RelaxDuration')\
        .na.fill(0)
    
    return data

def process_each_data(df):
    print("------------ => 3.2 Process category --------------")
    data = process_category(df)
    print("------------ => 3.3 Process group by --------------")
    data = process_groupby_data(data)
    print("------------ => 3.4 Pivot data --------------")
    data = process_pivot_data(data)
    return data

def process_data(path_list):
    data_result = None
    for path in path_list:
        print("------------ => 3.1 Read --------------")
        print("Path:", path)
        df = read_data(path)
        print("Data count:", df.count())
        df = process_each_data(df)
        print("------------ => 3.4 Merge data has been processed into data common --------------")
    
        if data_result is None:
            data_result = df
        else:
            data_result = data_result.unionAll(df)
            data_result.cache()

    return data_result

def main_task(path, stat_date, end_date):
    print("------------1. Get list days--------------")
    listDays = genarate_date_range(stat_date, end_date)
    print("------------2. Get new path--------------")
    list_path_new = get_new_path(path,listDays)
    print("------------3. Read and merge data form list--------------")
    data = process_data(list_path_new)
    data.show()
    print("------------4. Save data--------------")
    save_path = "D:\\WORKSPACE\\DE\\study_de\\Practice\\Class3_Class4\\Storage\\DataFrom30DayMethoad1"
    data.repartition(1).write.csv(save_path, header=True)
    print("------------Finish--------------")
    
   
path = 'D:\\WORKSPACE\\DE\\study_de\\Big_Data\\Items Shared on 4-29-2023\\Dataset\\log_content'
start_date = '20220401'
end_date = '20220430'
main_task(path, start_date, end_date)