# ------------------------------------------
# FUNCTION my_map
# ------------------------------------------
def my_map(my_input_stream, my_output_stream, my_mapper_input_parameters):
    # 1. I iterate trough the input stream and process each line and add
    # to the file all the data in the desired format
    for line in my_input_stream:
        # 2. I process each line in the csv
        project = process_line(line)

        # 3. Define the variables for a better understanding
        wikimedia = project[0]
        name = project[1]
        lang = project[2]
        views = int(project[3])

        # 4. Desired wikimedia_language, webpageName, views -> write it in the output file
        if views > 0:
            msg = str(wikimedia) + "_" + lang + "\t" + "(" + str(name) + "," + str(views) + ")" + "\n"
            my_output_stream.write(msg)

# ------------------------------------------
# FUNCTION my_reduce
# ------------------------------------------
def my_reduce(my_input_stream, my_output_stream, my_reducer_input_parameters):
    # 1. I create the necessary variables
    previous_wikimedia = ""
    previous_views = 0
    # 1.1 Add results to a list, this list will only contain the web-page per most views per project and language
    my_list = []

    # 2. I iterate through each line of the input file
    for line in my_input_stream:
        # 2.1. I get the info from the line
        data = get_key_value(line)
        current_wikimedia = data[0]
        subdata = data[1]
        # 2.2. I split the line by comma character
        params = subdata.split(",")
        name = params[0]
        current_views = params[1]

        # 2.3 First line
        if str(previous_wikimedia) == "":
            previous_wikimedia = str(current_wikimedia)
            previous_views = int(current_views)
        # 2.4 Remaining lines
        else:
            # 2.4.1 Same wikimedia -> continue, if has more views then we update the number of views
            if str(current_wikimedia) == str(previous_wikimedia):
                if int(current_views) > int(previous_views):
                    previous_views = current_views
            # 2.4.2. If the wikimedia is a new one
            else:
                # 2.4.2.1 Add previous wikimedia_lang and its views to the list
                msg = str(previous_wikimedia) + "\t" + "(" + str(name) + ", " + str(previous_views) + ")" + "\n"
                my_list.append(msg)
                # 2.4.2.2 I reset the previous wikimedia variable to be the new one
                previous_wikimedia = current_wikimedia
                previous_views = current_views

    # 3. I add to the list the very last wikimedia_lang and its webpageName and views
    if (previous_wikimedia != ""):
        msg = str(previous_wikimedia) + "\t" + "(" + str(name) + ", " + str(previous_views) + ")" + "\n"
        my_list.append(msg)

    # 4. Iterate through the list and write the data to the output file
    for each in my_list:
        my_output_stream.write(each + "\n")

# ------------------------------------------
# FUNCTION my_spark_core_model
# ------------------------------------------
def my_spark_core_model(sc, my_dataset_dir):
    
    # 1. Operation C1: Creation 'textFile', so as to store the content of the dataset contained in the folder dataset_dir into an RDD.
    inputRDD = sc.textFile(my_dataset_dir)

    # 2. Operation T1: Transformation 'map' and 'filter', so as to get a new RDD ('dataRDD') with all the data of inputRDD. 
    # I filter to take wikimedias with views grater than 0 because I think having data with 0 views is useless.
    dataRDD = inputRDD.map(lambda x : process_line(x)).filter(lambda x : int(x[3])>0)

    # 3. Operation T2: Transformation 'map', so as to get a new RDD ('mappedRDD'), with (Wikimedia_lang-name      views) per line of the data set
    mappedRDD = dataRDD.map(lambda x : (key(formatName(x[0], x[2]), x[1]), x[3])).distinct()

    # 4. Operation T4: Transformation 'groupByKey', 'mapValues', 'sortByKey', so as to get new RDD ('solutionRDD') 
    solutionRDD = mappedRDD.groupByKey().mapValues(max).sortBy(lambda x : int(x[1])*(-1))           # from mappedRDD with the maximum value (views) 
                                                                                                    # sorted in descending order or views
    # 5. Operation A1: 'collect'
    resVal = solutionRDD.collect()

    # 6. Print by the screen the collection computed in resVal
    for item in resVal:
      keyParams = item[0].split("-")
      project = keyParams[0]
      name = keyParams[1]
      msg = project + "\t" + "(" + name + ", " + item[1] + ")"
      print(msg)

# ------------------------------------------
# FUNCTION formatName
# ------------------------------------------
def formatName(wikimedia, language):
    res = ""

    res = wikimedia+"_"+language

    return res

# ------------------------------------------
# FUNCTION key
# ------------------------------------------
def key(wikimedia_lang, name):
    res = ""

    res = wikimedia_lang+"-"+name

    return res

# ------------------------------------------
# FUNCTION format
# ------------------------------------------
def format(wikimedia_langName, views):
    res = ""
    
    params = wikimedia_langName.split("-")
    project = params[0]
    name = params[1]
    res = project + "\t" + "(" + name +", " + views + ")"

    return res

# ------------------------------------------
# FUNCTION my_spark_streaming_model
# ------------------------------------------
def my_spark_streaming_model(ssc, monitoring_dir):
    
    # 1. Operation C1: textFileStream
    inputDStream = ssc.textFileStream(monitoring_dir)

    # 2. Operation T1: map
    allDataDStream = inputDStream.map(process_line)

    # 3. Operation T2: filter
    filteredDStream = allDataDStream.filter(lambda line : int(line[3]) > 0)

    # 4. Operation T3: map
    mappedDStream = filteredDStream.map(lambda x : (key(formatName(x[0], x[2]), x[1]), x[3]))

    # 5. Operation T4: groupByKey, mapValues and transform
    reducedDStream = mappedDStream.groupByKey().mapValues(max).transform(lambda rdd: rdd.sortBy(lambda item: int(item[1])*(-1)))

    # 6. Operation T5: map to get the desired format
    solutionDStream = reducedDStream.map(lambda x : format(x[0], x[1]))

    # 7. Operation A1: print
    solutionDStream.pprint()