# --------------------------------------------------------
#           PYTHON PROGRAM
# Here is where we are going to define our set of...
# - Imports
# - Global Variables
# - Functions
# ...to achieve the functionality required.
# When executing > python 'this_file'.py in a terminal,
# the Python interpreter will load our program,
# but it will execute nothing yet.
# --------------------------------------------------------

import sys
import codecs


# ------------------------------------------
# FUNCTION filter_languages
# ------------------------------------------
def filter_languages(line):
    res = False
    items = line.split(" ")
    language_code = items[0].split(".")

    for language in languages:

        if language == language_code[0]:
            print(language)
            res = True

    return res


# ------------------------------------------
# FUNCTION process_word
# ------------------------------------------
def process_items(line):
    res = ()

    items = line.split(" ")

    val1 = items[0]
    val2 = items[1]
    val3 = int(items[2])

    res = (val1, val2, val3)

    return sorted(res)


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(dataset_dir, o_file_dir, languages, num_top_entries):
    # We remove the solution directory, to rewrite into it
    dbutils.fs.rm(o_file_dir, True)

    # Creation 'textFile', so as to store the content of the dataset into an RDD.
    inputRDD = sc.textFile(dataset_dir)

    # We filter by language to return a new dataset, with only the languages we are interested in
    filteredRDD = inputRDD.filter(filter_languages)

    # We map the items of the line we are interested in
    mappedRDD = filteredRDD.map(lambda x: process_items(x))

    # We sort the rdd
    sortedRDD = mappedRDD.sortBy(lambda x: (x[0], x[2]), ascending=False)

    # We store the RDD solutionRDD into the desired folder from the DBFS
    sortedRDD.saveAsTextFile(o_file_dir)


# Complete the Spark Job


# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    dataset_dir = "/FileStore/tables/A01_my_dataset/"
    o_file_dir = "/FileStore/tables/A01_my_result/"

    languages = ["en", "es", "fr"]
    num_top_entries = 5

    my_main(dataset_dir, o_file_dir, languages, num_top_entries)

