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
# FUNCTION get_ran_outs
# ------------------------------------------
def get_ran_outs(my_list, measurement_time):
    # 1. We create the output variable
    res = []
    # res = None

    # 2. We compute the auxiliary lists:
    # date_values (mapping dates to minutes passed from midnight)
    # indexes (indexes of actual ran-outs)
    date_values = []

    for item in my_list:
        date_values.append((int(item[0:2]) * 60) + int(item[3:5]))

    # 3. We get just the real ran-outs
    index = len(my_list) - 1
    measurements = 1

    # 3.1. We traverse the indexes
    while (index > 0):
        # 3.1.1. If it is inside a ran-out cycle, we increase the measurements
        if (date_values[index - 1] == (date_values[index] - measurement_time)):
            measurements = measurements + 1
        # 3.1.2. Otherwise, we append the actual ran-out and re-start the measurements
        else:
            res.insert(0, (my_list[index], measurements))
            measurements = 1

        # 3.1.3. We decrease the index
        index = index - 1

    # 3.2. We add the first position
    res.insert(0, (my_list[index], measurements))

    # 5. We return res
    return res

# ------------------------------------------
# FUNCTION formatDate
# ------------------------------------------
def formatDate(date):
    # 1. Define output variable
    res = ""

    # 2. Split the date by day month and year
    params = date.split("-")
    day = params[0]
    month = params[1]
    year = params[2]

    # 3. Create a string with the date in the format year-month-day
    res = year + "-" + month + "-" + day

    # 4. Ouput final variable
    return res 
  
# ------------------------------------------


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc, my_dataset_dir, station_name, measurement_time):
     # 1. Operation C1: Creation 'textFile', so as to store the content of the dataset contained in the folder dataset_dir into an RDD.
    inputRDD = sc.textFile(my_dataset_dir)
    
    # 2. Operation T1: Transformation 'map' and 'filter', so as to get a new RDD ('dataRDD') with all the data of inputRDD.
    dataRDD = inputRDD.map(lambda x: process_line(x)).filter(lambda x: str(x[1])==station_name and x[0]=='0' and x[5]=='0')
    
    # 3. Operation T2: Transformation 'map', so as to get a new RDD ('dateRDD') with a pair (date, time) per line of the dataset.
    dateRDD = dataRDD.map(lambda x : (formatDate(x[4][0:10]), x[4][11:19]))
    
    # 4. Operation T3: Transformation 'groupByKey', so as to get a new RDD ('groupedRDD') from inputRDD.
    groupedRDD = dateRDD.groupByKey()
    
    # 5. Operation T4: Transformation 'map', so as to get a new RDD ('mapRDD') in the form (date[time1, time2, ..., timex-1, timex]) from the desire station.
    mapRDD = groupedRDD.map(lambda x : (x[0], list(x[1])))
    
    # 6. Operation T5: Transformation 'flatMapValues', so as to get a new RDD ('runoutsRDD') with a list of runouts.
      # 6.1. Operation T5.1: Transformation 'sortByKey'.
    runoutsRDD = mapRDD.flatMapValues(lambda x : (get_ran_outs(x, measurement_time))).sortByKey()
        
    # 7. Operation A1: 'collect'.
    resVal = runoutsRDD.collect()
    
    # 8. Print by the screen the collection computed in resVAL
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
    # 1. We use as many input parameters as needed
    station_name = "Fitzgerald's Park"
    measurement_time = 5

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
    my_main(sc, my_dataset_dir, station_name, measurement_time)