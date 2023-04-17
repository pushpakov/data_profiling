from flask import Flask
from services.de_duplication import *
from services.join_table import *
from services.ssequence_generator import *
from services.filter_table import *
from services.union_table import *

app = Flask(__name__)


@app.route('/de-dupe', methods=['GET']) 
def save_csv_file(): 
    return de_dupe_csv_file() 


@app.route('/join-table', methods=['GET'])
def join_operation_route():
      return join_operation() 


@app.route('/seq-gen', methods=['GET'])
def seq_generator_route():
      return sg_col_file() 


@app.route('/filter-csv', methods=['GET'])   
def filter_csv():
    return filter_csv_file() 


@app.route('/union-csv', methods=['GET'])   
def union_csv():
    return union_operation()


if __name__ == '__main__':
       app.run(debug=True, port=5000) 

