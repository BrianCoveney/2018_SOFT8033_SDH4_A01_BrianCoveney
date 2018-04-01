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

# ---------------------------------------
#  FUNCTION get_key_value
# ---------------------------------------
from _operator import itemgetter


def get_key_value(line):
    # Get rid of brackets
    line = line.replace("(", "")
    line = line.replace(")", "")

    # Split the string by the tabulator delimiter
    item_list = line.split("\t")

    # Get the key and the value and return them
    language = item_list[0]
    page_view = item_list[1]

    # Split the string by the page and page view count
    fields = page_view.rsplit(",", 1)

    # Get the key and the value and return them
    page_viewed = fields[0]
    num_views = int(fields[1])

    return language, page_viewed, num_views


def add_to_dict(wiki_list, wiki_dict):
    for item in wiki_list:
        if item[0] not in wiki_dict:
            page_with_view = (item[1], item[2])
            wiki_dict[item[0]] = [page_with_view]
        else:
            page_with_view = (item[1], item[2])
            wiki_dict[item[0]].append(page_with_view)
    return wiki_dict


def get_result_list(wiki_dict, n):
    result_list = []
    for key in wiki_dict:
        for index in range(0, n):
            if len(wiki_dict[key]) > index:
                result_string = key + "\t" + str(wiki_dict[key][index]) + "\n"
                result_string = result_string.replace("'", "")
                result_string = result_string.replace("\"", "")
                result_list.append(result_string)
    return result_list


# ------------------------------------------
# FUNCTION my_reduce
# ------------------------------------------
def my_reduce(input_stream, num_top_entries, output_stream):
    # Create our empty list and dictionary
    wiki_tuple_list = []
    wiki_dict = {}

    # Process the lines to fill our list
    for text_line in input_stream.readlines():
        wiki_tuples = get_key_value(text_line)
        wiki_tuple_list.append(wiki_tuples)

    # Sort the list first by language, then by language with project
    sorted_list = sorted(wiki_tuple_list, key=itemgetter(0, 2), reverse=True)

    # Call the function 'add_to_dict()' to populate our dictionary.
    #  - key   : language(project)
    #  - value : [(pageA, page_viewsA), (pageB, page_viewsB),...(pageN, page_viewsN)]
    wiki_dict = add_to_dict(sorted_list, wiki_dict)

    # Call the function 'get_result_list()' to populate a list with all language_project, page_viewed and num_views
    # to get the top five results
    output = get_result_list(wiki_dict, num_top_entries)

    # Print our list to the standard output
    for out in output:
        output_stream.write(out)

    pass


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(debug, i_file_name, o_file_name, num_top_entries):
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
    my_reduce(my_input_stream, num_top_entries, my_output_stream)


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

    i_file_name = "sort_simulation.txt"
    o_file_name = "reduce_simulation.txt"

    num_top_entries = 5

    my_main(debug, i_file_name, o_file_name, num_top_entries)
