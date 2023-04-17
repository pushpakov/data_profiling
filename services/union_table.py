from flask import request, jsonify
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from multiprocessing import Pool
from logs.logger import setup_logger

logger = setup_logger()

spark = SparkSession.builder.master("local[1]").appName(
    "de_duplication.com").getOrCreate()


def union_operation():
    try:
        json_data = request.get_json()

        # Get the tables from the JSON data
        table1 = spark.read.options(header='True', delimiter=',').csv(
            json_data['table1_path'])
        table2 = spark.read.options(header='True', delimiter=',').csv(
            json_data['table2_path'])

        # Select only the columns needed for the union from each table
        table1_columns = json_data['table1_columns'] 
        table2_columns = json_data['table2_columns'] 

        table1.createOrReplaceTempView("table1")
        table2.createOrReplaceTempView("table2")

        select_t1 = []
        select_t2 = [] 
        prefix_t1= "table1."
        prefix_t2 = "table2."

        for colm in table1_columns:
            select_t1.append(prefix_t1+colm+f" as t1_{colm} ")

        for colm in table2_columns:
            select_t2.append(prefix_t2+colm+f" as t2_{colm} ") 

        res1 = ", ".join(select_t1) 
        res2 = ", ".join(select_t2) 

        
        # create the SQL query string to perform union
        query = f"""
        SELECT {res1}
        FROM table1
        UNION ALL
        SELECT {res2} 
        FROM table2
        """

        # execute the SQL query using spark.sql
        resulted_table = spark.sql(query) 

        resulted_table.show(truncate=False) 
        resulted_table.write.format("csv").mode('overwrite').save( 
            "/home/allbanero/Downloads/unioned_table")

        return jsonify({'message': 'Tables unioned successfully.'}) 

    except Exception as e:
        logger.error('An error occurred while unioning the CSV files. ' + str(e), 400)  
        return jsonify({'error': 'An error occurred while unioning the CSV files.'}),400 
