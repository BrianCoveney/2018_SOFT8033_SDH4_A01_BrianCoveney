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
from operator import itemgetter


# ------------------------------------------
# FUNCTION get_language
# ------------------------------------------
def get_language(line, languages):
    language_project = line[0].split(".")
    for language in languages:
        if language in language_project:
            return language


# ------------------------------------------
# FUNCTION process_items
# ------------------------------------------
def process_items(line):
    items = line.split(" ")
    lang = items[0]
    page = items[1]
    views = int(items[2])

    return lang, (page, views)


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(dataset_dir, o_file_dir, languages, num_top_entries):
    # We remove the solution directory, to rewrite into it
    dbutils.fs.rm(o_file_dir, True)

    # Creation 'textFile', so as to store the content of the dataset into an RDD.
    inputRDD = sc.textFile(dataset_dir)

    # Return a new RDD by applying a function to each element of this RDD
    mapRDD = inputRDD.map(lambda x: process_items(x))

    # Return a new RDD containing only the elements that satisfy our predicate, i.e. languages = ["en", "es", "fr"]
    filterRDD = mapRDD.filter(lambda x: get_language(x, languages))

    # Group the RDD by 'language_project'
    groupByKeyRDD = filterRDD.groupByKey()

    # Sorts the RDD by 'language_project'
    sortByKeyRDD = groupByKeyRDD.sortByKey()

    # Pass each value in the key-value pair RDD through a map function without changing the keys.
    # Sort the RDD by 'views' with at limit of 'num_top_entries'
    solutionRDD = sortByKeyRDD.flatMapValues(
        lambda x: sorted(list(x), key=itemgetter(1), reverse=True)[:num_top_entries])

    # We store the RDD solutionRDD into the desired folder from the DBFS
    solutionRDD.saveAsTextFile(o_file_dir)


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
