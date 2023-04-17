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
        join_condition = json_data['join_condition']
        print(join_condition)

        # Set the where condition
        how_condition = json_data['how_condition']  
        print(how_condition)

        # Get the tables from the JSON data
        table1 = spark.read.options(header='True', delimiter=',').csv(
            json_data['table1_path'])
        table2 = spark.read.options(header='True', delimiter=',').csv(
            json_data['table2_path'])

        # Select only the columns needed for the join from each table
        table1_columns = json_data['table1_columns'] 
        table2_columns = json_data['table2_columns'] 

        table1.createOrReplaceTempView("table1")
        table2.createOrReplaceTempView("table2")

        select_cond = []
        prefix_t1= "table1."
        prefix_t2 = "table2."

        for colm in table1_columns:
            select_cond.append(prefix_t1+colm+f" as t1_{colm} ")

        for colm in table2_columns:
            select_cond.append(prefix_t2+colm+f" as t2_{colm} ") 
        print("---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------")
        print(*select_cond)
        res = ", ".join(select_cond)  
        print(res)
        
        # create the SQL query string using the variables for the conditions
        query = f"""
        SELECT {res} 
        FROM table1
        JOIN table2 
        ON {join_condition}
        """ 

        # execute the SQL query using spark.sql
        resulted_table = spark.sql(query) 

        resulted_table.show(truncate=False) 
        resulted_table.write.format("csv").mode('overwrite').save( 
            "/home/allbanero/Downloads/joined_table")

        # Convert the result to JSON and return it
        return jsonify({'message': 'Table joined successfully.'})

    except Exception as e:
        logger.error('An error occurred while joining the CSV file. ' + str(e), 400)  
        return jsonify({'error': 'An error occurred while joining the CSV file.'}),400 


