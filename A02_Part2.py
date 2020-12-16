# --------------------------------------------------------
#
# PYTHON PROGRAM DEFINITION
#
# The knowledge a computer has of Python can be specified in 3 levels:
# (1) Prelude knowledge --> The computer has it by default.
# (2) Borrowed knowledge --> The computer gets this knowledge from 3rd party libraries defined by others
#                            (but imported by us in this program).
# (3) Generated knowledge --> The computer gets this knowledge from the new functions defined by us in this program.
#
# When launching in a terminal the command:
# user:~$ python3 this_file.py
# our computer first processes this PYTHON PROGRAM DEFINITION section of the file.
# On it, our computer enhances its Python knowledge from levels (2) and (3) with the imports and new functions
# defined in the program. However, it still does not execute anything.
#
# --------------------------------------------------------

# ------------------------------------------
# IMPORTS
# ------------------------------------------
import pyspark

import calendar
import datetime

# ------------------------------------------
# FUNCTION get_day_of_week
# ------------------------------------------
# Examples: get_day_of_week("06-02-2017") --> "Monday"
#           get_day_of_week("07-02-2017") --> "Tuesday"

def get_day_of_week(date):
    # 1. We create the output variable
    res = calendar.day_name[(datetime.datetime.strptime(date, "%d-%m-%Y")).weekday()]

    # 2. We return res
    return res

# ------------------------------------------
# FUNCTION process_line
# ------------------------------------------
def process_line(line):
    # 1. We create the output variable
    res = ()

    # 2. We remove the end of line character
    line = line.replace("\n", "")

    # 3. We split the line by tabulator characters
    params = line.split(";")

    # 4. We assign res
    if (len(params) == 7):
        res = tuple(params)

    # 5. We return res
    return res

# ------------------------------------------
# FUNCTION processDate_line
# ------------------------------------------
def processDate_line(line):
    # 1. We create the output variable
    res = ()

    # 2. We remove the end of line character
    line = line.replace("\n", "")

    # 3. We split the line by tabulator characters
    params = line.split(" ")

    # 4. We assign res
    if (len(params) == 2):
        res = tuple(params)

    # 5. We return res
    return res
  
# ------------------------------------------
# FUNCTION getNameAndTime
# ------------------------------------------
def getNameAndTime(list):
    # 1. We create the output variable
    res = ""
    
    # 2. Assign the day of the week
    weekday = get_day_of_week(list[0])
    
    # 3. We split the time by : character
    params = list[1].split(":")

    # 4. Assign res
    res = weekday + "_" + params[0]

    # 5. We return res
    return res

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc, my_dataset_dir, station_name):
    
    # 1. Operation C1: Creation 'textFile', so as to store the content of the dataset contained in the folder dataset_dir into an RDD.
    inputRDD = sc.textFile(my_dataset_dir)
    
    # 2. Operation T1: Transformation 'map' and 'filter', so as to get a new RDD ('dataRDD') with all the data of inputRDD.
    dataRDD = inputRDD.map(lambda x: process_line(x)).filter(lambda x: str(x[1])==station_name and x[0]=='0' and x[5]=='0')
    
    # 3. Operation P1: We 'persist' dataRDD
    dataRDD.persist()
    
    # 4. Operation A1: We 'count' how many stations are in the dataset (i.e., in dataRDD)
    totalCount = dataRDD.count()
    
    # 5. Operation T2: Transformation 'map', so as to get a new RDD ('dateRDD') with a pair (weekday_time, 1) per line of the dataset.
    dateRDD = dataRDD.map(lambda x: (getNameAndTime(processDate_line(x[4])), 1))
    
    # 6. Operation T3: Transformation 'combineByKey', so as to get a new RDD ('totalInfoRDD') with a pair (weekday_time, (totalCount, totalCount)) per line of the data. 
      # 6.1. Operation T4: Transformation 'sortBy', so as to get 'dateRDD' sorted by the desired order we want.
    totalInfoRDD = dateRDD.combineByKey(lambda x: (x, 1),
                                        lambda x, y: (x[0] + y, x[1] + 1),
                                        lambda x, y: (x[0] + y[0], x[1] + y[1])).sortBy(lambda x: (-1) * x[1][0])
    
    # 7. Operation T5: Transformation 'mapValues', so as to get the percentage for weekday_time in a new RDD ('solutionRDD').
    solutionRDD = totalInfoRDD.mapValues( lambda value: (value[0], ((value[1] * 1.0) / (totalCount * 1.0) * 100.0)))
    
    # 8. Operation A2: 'collect'.
    resVal = solutionRDD.collect()
    
    # 9. Print by the screen the collection computed in resVAL
    for item in resVal:
      print(item)

# --------------------------------------------------------
#
# PYTHON PROGRAM EXECUTION
#
# Once our computer has finished processing the PYTHON PROGRAM DEFINITION section its knowledge is set.
# Now its time to apply this knowledge.
#
# When launching in a terminal the command:
# user:~$ python3 this_file.py
# our computer finally processes this PYTHON PROGRAM EXECUTION section, which:
# (i) Specifies the function F to be executed.
# (ii) Define any input parameter such this function F has to be called with.
#
# --------------------------------------------------------
if __name__ == '__main__':
    # 1. We use as many input arguments as needed
    station_name = "Fitzgerald's Park"

    # 2. Local or Databricks
    local_False_databricks_True = True

    # 3. We set the path to my_dataset
    my_local_path = "/home/nacho/CIT/Tools/MyCode/Spark/"
    my_databricks_path = "/"

    my_dataset_dir = "FileStore/tables/3_Assignment/my_dataset/"

    if local_False_databricks_True == False:
        my_dataset_dir = my_local_path + my_dataset_dir
    else:
        my_dataset_dir = my_databricks_path + my_dataset_dir

    # 4. We configure the Spark Context
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    print("\n\n\n")

    # 5. We call to our main function
    my_main(sc, my_dataset_dir, station_name)