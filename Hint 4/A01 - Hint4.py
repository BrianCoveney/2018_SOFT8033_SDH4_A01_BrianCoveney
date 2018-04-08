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
    # Split the string by whitespace
    items = line.split(" ")

    num_views = int(items[2])

    language_code = items[0].split(".")
    lang = language_code[0]

    return lang, num_views


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(dataset_dir, o_file_dir, per_language_or_project):
    # 1. We remove the solution directory, to rewrite into it
    dbutils.fs.rm(o_file_dir, True)

    inputRDD = sc.textFile(dataset_dir)

    mappedRDD = inputRDD.map(get_key_value)

    mappedRDD.persist()

    total = mappedRDD.values().sum()

    mappedRDD = mappedRDD.groupByKey()

    solutionRDD = mappedRDD.mapValues(lambda x: (float(sum(x)) / float(total) * 100))

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
