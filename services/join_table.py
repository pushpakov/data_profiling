from flask import request, jsonify
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from multiprocessing import Pool
import asyncio


spark = SparkSession.builder.master("local[1]").appName("de_duplication.com").getOrCreate()


from pyspark.sql.functions import col

def inner_joined_file(df1, df2, table1_columns, table2_columns, join_conditions, where_condition):
    # select columns from the first table
    df1_select = df1.select([col(c) for c in table1_columns])
    # select columns from the second table
    df2_select = df2.select([col(c) for c in table2_columns])
    # perform inner join operation
    joined_df = df1_select.join(df2_select, join_conditions, "inner")
    # apply where condition
    if where_condition:
        joined_df = joined_df.filter(where_condition)
    return joined_df

    
def left_outer_joined_file(df1, df2, table1_columns, table2_columns, join_conditions, where_condition): 
    # perform inner join operation
    pass
    
def right_outer_joined_file(df1, df2, table1_columns, table2_columns, join_conditions, where_condition): 
    # perform inner join operation
    pass
    
def full_outer_joined_file(df1, df2, table1_columns, table2_columns,join_conditions, where_condition):    
    # perform inner join operation
    pass
    



def join_operation():
    try:
        json_data = request.get_json()

        file1_path = json_data['file1_path']
        file2_path = json_data['file2_path']

        input_col1 = json_data["input_col1"]  # this is a list which will have columns from user for joining
        input_col2 = json_data["input_col2"]  # this is a list which will have columns from user for joining 

        join_conditions = json_data['join_conditions']   

        where_condition = json_data['where_conditions'] 


        join_operation = json_data['operation'] 



        df_csv_file1 = spark.read.options(header='True', delimiter=',').csv(file1_path)
        df_csv_file2 = spark.read.options(header='True', delimiter=',').csv(file2_path)   


        table1_columns = input_col1['columns'].split(',')
        table2_columns = input_col2['columns'].split(',')      

        if join_operation == 1:
            inner_join = inner_joined_file(df_csv_file1, df_csv_file2, table1_columns, table2_columns, join_conditions, where_condition) 
            inner_join.write.format("csv").mode('overwrite').save("/home/allbanero/Downloads/inner_joined_file")
            inner_joined_rows = inner_join.count()

        if join_operation == 2:
            left_outer_join = left_outer_joined_file(df_csv_file1, df_csv_file2, table1_columns, table2_columns, join_conditions, where_condition) 
            left_outer_join.write.format("csv").mode('overwrite').save("/home/allbanero/Downloads/left_outer_joined_file") 


        if join_operation == 3:
            right_outer_join = right_outer_joined_file(df_csv_file1, df_csv_file2, table1_columns, table2_columns, join_conditions, where_condition) 
            right_outer_join.write.format("csv").mode('overwrite').save("/home/allbanero/Downloads/right_outer_joined_file")


        if join_operation == 4:
            full_outer_join = full_outer_joined_file(df_csv_file1, df_csv_file2, table1_columns, table2_columns, join_conditions, where_condition) 
            full_outer_join.write.format("csv").mode('overwrite').save("/home/allbanero/Downloads/full_outer_joined_file")



        total_rows_table1 = (df_csv_file1.count())
        total_rows_table2 = (df_csv_file2.count())

        # return a JSON object with a success message
        return jsonify({'message': 'CSV file was joined successfully.', "inner_joined rows :": inner_joined_rows, "table2_rows": total_rows_table2})   
    except Exception as e:
        print(e)
        # return a JSON object with an error message
        return jsonify({'error': 'An error occurred while deduplicating the CSV file.'})
