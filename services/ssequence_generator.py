from flask import request, jsonify
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from logs.logger import setup_logger

logger = setup_logger()


spark = SparkSession.builder.master("local[1]").appName(
    "de_duplication.com").getOrCreate()


from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

def add_sequence_generator_column(df, num_digits):
    # Generate a sequence number for each row
    window = Window.orderBy('id')
    df_with_sequence = df.withColumn('sequence', row_number().over(window))

    # Add leading zeros to the sequence number
    df_with_sequence = df_with_sequence.withColumn(
        'sequence generator', 
        concat(
            lpad(df_with_sequence['sequence'], num_digits, '0'),
            lit('_SG')
        )
    )
    return df_with_sequence.drop('sequence')



def sg_col_file():
    try:
        json_data = request.get_json()

        # Select the columns we want to join on
        digit_for_seq = json_data['digit']
        print(digit_for_seq)

        # Get the tables from the JSON data
        table1 = spark.read.options(header='True', delimiter=',').csv(
            json_data['table_path'])  
        
                # Add sequence generator column with 5 digits
        df_with_sequence = add_sequence_generator_column(table1, digit_for_seq)   

        df_with_sequence.show()   

        # Save the resulting DataFrame to CSV file
        df_with_sequence.write.format("csv").mode('overwrite').save(
            "/home/allbanero/Downloads/sequence_file")
        
        return jsonify({'message': 'seq generated added successfully.'})  
   
                
    except Exception as e:
        logger.error(str(e))
        # return a JSON object with an error message
        return jsonify({'error': 'An error occurred while deduplicating the CSV file.'}),400 
    


