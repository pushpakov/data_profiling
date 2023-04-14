from flask import Flask
from services.de_duplication import *
from services.join_table import *

app = Flask(__name__)


@app.route('/de_dupe', methods=['GET']) 
def save_csv_file(): 
    return de_dupe_csv_file() 


@app.route('/join_table', methods=['GET'])
def join_operation_route():
      return join_operation() 


if __name__ == '__main__':
       app.run(debug=True, port=5000) 

