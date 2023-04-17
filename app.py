from flask import Flask
from services.de_duplication import *
from services.join_table import *
from services.ssequence_generator import *

app = Flask(__name__)


@app.route('/de_dupe', methods=['GET']) 
def save_csv_file(): 
    return de_dupe_csv_file() 


@app.route('/join_table', methods=['GET'])
def join_operation_route():
      return join_operation() 

@app.route('/seq-gen', methods=['GET'])
def seq_generator_route():
      return sg_col_file() 


if __name__ == '__main__':
       app.run(debug=True, port=5000) 

