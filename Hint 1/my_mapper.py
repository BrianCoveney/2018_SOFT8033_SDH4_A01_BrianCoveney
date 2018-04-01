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
from operator import itemgetter


def get_key_value(line):
    # Split the string by whitespace
    items = line.split(" ")

    # Get values
    language_project = items[0]
    page_viewed = items[1]
    num_views = int(items[2])

    # Split the string by language code, i.e. language without project
    language_code = items[0].split(".")

    return language_code, language_project, page_viewed, num_views


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
                result = result_string.replace("'", "")
                result_list.append(result)
    return result_list


# ------------------------------------------
# FUNCTION my_map
# ------------------------------------------
def my_map(input_stream, languages, num_top_entries, output_stream):
    # Create our empty list and dictionary
    wiki_list = []
    wiki_dict = {}

    # Process the lines to fill our list
    for text_line in input_stream:
        (language_code, language_project, page_viewed, num_views) = get_key_value(text_line)

        for language in languages:
            if language == language_project or language == language_code[0]:
                entry = language_project, page_viewed, num_views
                wiki_list.append(entry)

    # Sort the list first by language, then by language with project
    sorted_list = sorted(wiki_list, key=itemgetter(0, 2), reverse=True)

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
def my_main(debug, i_file_name, o_file_name, languages, num_top_entries):
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
    my_map(my_input_stream, languages, num_top_entries, my_output_stream)


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
    # i_file_name = "../my_dataset/pageviews-20180219-100000_0.txt"

    # -- Nacho's path to file -- #
    i_file_name = "pageviews-20180219-100000_0.txt"

    o_file_name = "mapResult.txt"

    languages = ["en", "es", "fr"]
    num_top_entries = 5

    # 2. Call to the function
    my_main(debug, i_file_name, o_file_name, languages, num_top_entries)
