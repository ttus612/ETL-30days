import os 
import datetime
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def main_task(path):

    print("-----------------------------------")
    print("Reading data from file")
    print("-----------------------------------")
	
    print("-----------------------------------")
    print("Path: " + path)
    print("-----------------------------------")

    print("-----------------------------------")
    ds = spark.read.json(path)
    print("Read data success")
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
    
    return ds
	

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
	
if __name__ == "__main__":
    findspark.init()

    spark = SparkSession.builder.config("spark.driver.memory", "8g").getOrCreate()

    path = 'D:\\WORKSPACE\\DE\\study_de\\Big_Data\\Items Shared on 4-29-2023\\Dataset\\log_content'

    # Dùng để lấy tất cả tên của fil trong thư mục
    # list_files = os.listdir(path)

    # Chọn ngày mình muốn xử lý
    list_files = genarate_date_range('20220401', '20220403')

    print("-----------------------------------")
    print("Start processing data")
    print("-----------------------------------")


    final_result = None
    for file_name in list_files:
        print(f"==> Processing file {file_name} <==")
        path_new = path + '\\' + file_name + '.json'
        processed_data = main_task(path_new)
        if final_result is None:
            final_result = processed_data
        else:
            final_result = final_result.unionAll(processed_data)

    print("-----------------------------------")
    print("Show data & structure after read all files")
    print("-----------------------------------")
    final_result.show()

    print("-----------------------------------")
    print("Group by data")
    print("-----------------------------------")
    grouped_data = final_result.groupBy('_source.Contract', 'Category').agg(
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
        .select(
            'Contract',
            'Giải trí', 'Phim ảnh', 'Trẻ em', 'Thể thao', 'Truyền hình')\
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

    save_path = "D:\\WORKSPACE\\DE\\study_de\\Practice\\Class3_Class4\\Storage\\Methoad2"
    pivot_data.repartition(1).write.csv(save_path, header=True)
    # pivot_data.coalesce(1).write.option("header","true").format("csv").save(save_path)

    print("-----------------------------------")
    print("End processing data")
    print("-----------------------------------")
		