from flask import request, jsonify
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from multiprocessing import Pool
import asyncio


spark = SparkSession.builder.master("local[1]").appName(
    "de_duplication.com").getOrCreate()


def deduplicate_columns(df, columns):
    if(columns == ['']):
        df_deduplicated = df.dropDuplicates()
    else: 
        df_deduplicated = df.dropDuplicates(subset=columns)
    return df_deduplicated


def dropped_file(source_df, dropped_df):

    # perform antijoin operation
    dropped_file = source_df.join(dropped_df, on=['ID'], how='left_anti')
    return dropped_file


def de_dupe_csv_file():
    try:
        # file = request.files['csv_file']
        # file_path = file.save("/home/downloads/test.csv")
        df_csv_file = spark.read.options(header='True', delimiter=',').csv(
            "/home/allbanero/Downloads/test.csv")

        input_data = request.form.to_dict()
        total_rows = (df_csv_file.count())
        # extract the columns to deduplicate from the input data
        dedup_columns = input_data['columns'].split(',')
        print(dedup_columns)   
        deduped_table = deduplicate_columns(df_csv_file, dedup_columns)

        deduped_table.write.format("csv").mode('overwrite').save(
            "/home/allbanero/Downloads/deduped_file")

        dropped_table = dropped_file(df_csv_file, deduped_table)

        dropped_table.write.format("csv").mode('overwrite').save(
            "/home/allbanero/Downloads/dropped_file")

        dropped_rows = dropped_table.count()
        # print(deduped_table.count())
        deduped_rows = deduped_table.count()
        # deduped_table.show()

        # return a JSON object with a success message
        return jsonify({'message': 'CSV file was deduplicated successfully.', "total rows :": total_rows, "deduped_rows": deduped_rows, "dropped_rows :": dropped_rows})
    except Exception as e:
        print(e)
        # return a JSON object with an error message
        return jsonify({'error': 'An error occurred while deduplicating the CSV file.'})
