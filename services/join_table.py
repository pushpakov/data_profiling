from flask import request, jsonify
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from multiprocessing import Pool
from logs.logger import setup_logger

logger = setup_logger()


spark = SparkSession.builder.master("local[1]").appName(
    "de_duplication.com").getOrCreate()

def join_operation():
    try:
        json_data = request.get_json()

        # Select the columns we want to join on
        join_columns = json_data['join_columns']
        print(join_columns)

        # Set the where condition
        how_condition = json_data['how_condition']  
        print(how_condition)

        # Get the tables from the JSON data
        table1 = spark.read.options(header='True', delimiter=',').csv(
            json_data['table1_path'])
        table2 = spark.read.options(header='True', delimiter=',').csv(
            json_data['table2_path'])

       
        # Perform the join operation
        joined_table = table1.join(table2, on=join_columns, how=how_condition)

        joined_table.write.format("csv").mode('overwrite').save( 
            "/home/allbanero/Downloads/joined_table")

        # Convert the result to JSON and return it
        return joined_table.toJSON().collect()

    except Exception as e:
         logger.error('An error occurred while joining the CSV file. ' + str(e), 400)  
         return jsonify({'error': 'An error occurred while joining the CSV file.'}),400 