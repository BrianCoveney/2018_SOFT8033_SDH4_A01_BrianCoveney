#!/usr/bin/python

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

from _operator import itemgetter


# ---------------------------------------
#  FUNCTION get_key_value
# ---------------------------------------
def get_key_value(line):
    items = line.split(" ")
    num_views = int(items[2])
    language_project = items[0].split(".")
    language = language_project[0]

    if len(language_project) == 1:
        project = "wikipedia"
    else:
        project = language_project[1]

    return language, project, num_views


# ------------------------------------------
# FUNCTION my_map
# ------------------------------------------
def my_map(input_stream, per_language_or_project, output_stream):
    wiki_dict = {}
    count = 0
    for text_line in input_stream:

        language, project, num_views = get_key_value(text_line)

        # When 'per_language_or_project' is set to True 'wiki_dict' contains languages.
        # Otherwise 'wiki_dict' contains 'projects'.
        if per_language_or_project:
            if language not in wiki_dict:
                wiki_dict[language] = count
            else:
                wiki_dict[language] += num_views
        elif per_language_or_project:
            if project not in wiki_dict:
                wiki_dict[project] = count
            else:
                wiki_dict[project] += num_views

    # Sort the dictionary by the value, i.e the count
    wiki_sorted = sorted(wiki_dict.items(), key=itemgetter(1), reverse=True)

    for w in wiki_sorted:
        language = w[0]
        count = w[1]
        out = language + "\t" + str(count) + "\n"
        output_stream.write(out)

    pass


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(debug, i_file_name, o_file_name, per_language_or_project):
    # We pick the working mode:

    # Mode 1: Debug --> We pick a file to read test the program on it
    if debug == True:
        my_input_stream = codecs.open(i_file_name, "r", encoding='utf-8')
        my_output_stream = codecs.open(o_file_name, "w", encoding='utf-8')
    # Mode 2: Actual MapReduce --> We pick std.stdin and std.stdout
    else:
        my_input_stream = sys.stdin
        my_output_stream = sys.stdout

    # We launch the Map program
    my_map(my_input_stream, per_language_or_project, my_output_stream)


# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. Input parameters
    debug = True

    # --- My path to file -- #
    i_file_name = "../../my_dataset/pageviews-20180219-100000_0.txt"

    # -- Nacho's path to file -- #
    # i_file_name = "pageviews-20180219-100000_0.txt"

    o_file_name = "mapResult.txt"

    per_language_or_project = True  # True for language and False for project

    # 2. Call to the function
    my_main(debug, i_file_name, o_file_name, per_language_or_project)
