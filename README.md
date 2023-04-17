# data_profiling
This repo is used to perform some task based on data profiling for csv files and the profiling includes de-duplication, join, union , filter, and sequence generator using pyspark .




data = request.get_json()

data_name =  data["name"]
data_age =  data["age"] 


found_data = collection.find({"$or":[{full_name:data_name}, {age:data_age}]})    
