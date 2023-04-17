from flask import request, jsonify
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from logs.logger import setup_logger

logger = setup_logger()


spark = SparkSession.builder.master("local[1]").appName(
    "de_duplication.com").getOrCreate()



def filter_csv_file():
    try:
        json_data = request.get_json()

        # Load the CSV file as a Spark DataFrame
        df = spark.read.options(header='True', delimiter=',').csv(json_data['file_path']) 

        # Select the desired columns based on the input JSON data
        columns_to_select = json_data['columns_to_select']
        selected_df = df.select(*columns_to_select)

        # Apply the basic filter if specified in the input JSON data
        basic_filter = json_data.get('basic_filter')
        if basic_filter:
            filtered_df = selected_df.filter(basic_filter)

        # Apply the advanced filter (using SQL) if specified in the input JSON data
        sql_query = json_data.get('sql_query')
        if sql_query:
            filtered_df = selected_df.filter(sql_query)

        # Order the resulting DataFrame based on the input JSON data
        order_by = json_data.get('order_by')
        if order_by:
            filtered_df = filtered_df.orderBy(*order_by)

        # Save the resulting DataFrame to a CSV file
        filtered_df.write.format("csv").mode('overwrite').save("/home/allbanero/Downloads/filtered_file")
        filtered_df.show()    
        
        return jsonify({'message': 'Filtering completed successfully.'})

    except Exception as e:
        logger.error(str(e))
        return jsonify({'error': 'An error occurred while filtering the CSV file.'}), 400
