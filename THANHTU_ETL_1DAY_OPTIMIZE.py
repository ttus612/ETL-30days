import findspark
findspark.init()

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder.config("spark.driver.memory", "8g").getOrCreate()

def main_task(path, save_path):

    print("-----------------------------------")
    print("Start processing data")
    print("-----------------------------------")

    print("-----------------------------------")
    print("Reading data from file")
    print("-----------------------------------")

    print("-----------------------------------")
    ds = spark.read.json(path+'20220410'+'.json')
    print("Read data success")
    print("-----------------------------------")

    print("-----------------------------------")
    print("Show data & structure")
    print("-----------------------------------")
    ds.show()
    ds.printSchema()
    ds.select('_source').printSchema()
    
    print("-----------------------------------")
    print("Processing data")
    print("-----------------------------------")
    ds = ds.withColumn('Category',
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
    
    print("-----------------------------------")
    print("Show data & structure after processing")
    print("-----------------------------------")
    ds.show()
   
    print("-----------------------------------")
    print("Group by data")
    print("-----------------------------------")
    grouped_data = ds.groupBy('_source.Contract', 'Category').agg(
        sum('_source.TotalDuration').alias('TotalDuration')
    )
    
    print("-----------------------------------")
    print("Show grouped data")
    print("-----------------------------------")
    grouped_data.show()

    print("-----------------------------------")
    print("Pivot data ")
    print("-----------------------------------")
    pivot_data = grouped_data.groupBy('Contract').pivot('Category')\
        .agg(sum('TotalDuration'))\
        .withColumnRenamed('Giải trí', 'TVDuration')\
        .withColumnRenamed('Phim ảnh', 'MovieDuration')\
        .withColumnRenamed('Trẻ em', 'ChildDuration')\
        .withColumnRenamed('Thể thao', 'SportDuration')\
        .withColumnRenamed('Truyền hình', 'RelaxDuration')\
        .na.fill(0)
    
    print("-----------------------------------")
    print("Show pivoted data")
    print("-----------------------------------")
    pivot_data.show()

    print("-----------------------------------")
    print("Save data")
    print("-----------------------------------")
    pivot_data.repartition(1).write.csv(save_path, header=True)
    # pivot_data.coalesce(1).write.option("header","true").format("csv").save(save_path)

    print("-----------------------------------")
    return print("==> Finish processing data")

    
if __name__ == "__main__":
    path = "D:\\WORKSPACE\\DE\\study_de\\Big_Data\\Items Shared on 4-29-2023\\Dataset\\log_content\\"
    save_path = "D:\\WORKSPACE\\DE\\study_de\\Practice\\Class3\\Thanh_Tu_ETL\\Store_ETL_2Day"
    main_task(path, save_path)