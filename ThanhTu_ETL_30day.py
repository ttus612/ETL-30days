import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def process_data(file_path):
    ds = spark.read.json(file_path)

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

    grouped_data = ds.groupBy('_source.Contract', 'Category').agg(
        sum('_source.TotalDuration').alias('TotalDuration')
    )

    pivot_data = grouped_data.groupBy('Contract').pivot('Category').agg(
        sum('TotalDuration')
    ).select(
        'Contract',
        'Giải trí', 'Phim ảnh', 'Trẻ em', 'Thể thao', 'Truyền hình'
    ).withColumnRenamed('Giải trí', 'TVDuration'
    ).withColumnRenamed('Phim ảnh', 'MovieDuration'
    ).withColumnRenamed('Trẻ em', 'ChildDuration'
    ).withColumnRenamed('Thể thao', 'SportDuration'
    ).withColumnRenamed('Truyền hình','RelaxDuration'
    ).na.fill(0)

    return pivot_data

if __name__ == "__main__":
    findspark.init()

    spark = SparkSession.builder.config("spark.driver.memory", "8g").getOrCreate()

    #Chỉnh lại đường dẫn file cuả bạn
    file_names = [f'D:\\WORKSPACE\\DE\\study_de\\Big_Data\\Items Shared on 4-29-2023\\Dataset\\log_content\\202204{str(i).zfill(2)}.json' for i in range(1, 31)]

    final_result = None

    for file_name in file_names:
        print(f"Processing file {file_name}")
        processed_data = process_data(file_name)

        if final_result is None:
            final_result = processed_data
        else:
            final_result = final_result.union(processed_data)


    final_result.show()
