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
# FUNCTION get_key_value
# ------------------------------------------
def get_key_value(line):
    items = line.split(" ")
    num_views = int(items[2])
    language_code = items[0].split(".")
    lang = language_code[0]
    return lang, num_views


# ------------------------------------------
# FUNCTION get_percentage
# ------------------------------------------
def get_percentage(x, sumViews):
    percentage = (str(float(sum(x)) / float(sumViews) * 100) + "%")
    return percentage


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(dataset_dir, o_file_dir, per_language_or_project):
    # We remove the solution directory, to rewrite into it
    dbutils.fs.rm(o_file_dir, True)

    # Creation 'textFile', so as to store the content of the dataset into an RDD.
    inputRDD = sc.textFile(dataset_dir)

    # Return a new RDD by applying a function to each element of this RDD
    mappedRDD = inputRDD.map(get_key_value)

    # Persist mappedRDD, as we are going to use it more than once
    mappedRDD.persist()

    # Get the sum of page views
    sumViews = mappedRDD.values().sum()

    # Group the RDD
    mappedRDD = mappedRDD.groupByKey()

    # Map values with a percentage of page views per language
    solutionRDD = mappedRDD.mapValues(lambda x: get_percentage(x, sumViews))

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

    per_language_or_project = True  # True for language and False for project

    my_main(dataset_dir, o_file_dir, per_language_or_project)
