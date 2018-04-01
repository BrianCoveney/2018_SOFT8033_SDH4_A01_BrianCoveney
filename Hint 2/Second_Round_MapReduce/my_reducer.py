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


def get_key_value(line):
    # Split the string by tabulator
    items = line.split("\t")

    # Get values and return them
    lang = items[0]
    num_views = int(items[1])

    return lang, num_views

# ------------------------------------------
# FUNCTION my_reduce
# ------------------------------------------
def my_reduce(input_stream, total_petitions, output_stream):
    wiki_dict = {}

    lang_value = 0
    for text_line in input_stream:
        lang_key, num_views = get_key_value(text_line)

        if lang_key not in wiki_dict:
            wiki_dict[lang_key] = lang_value
        else:
            wiki_dict[lang_key] += num_views

    wiki_sorted = sorted(wiki_dict.items(), key=itemgetter(1), reverse=True)

    for s in wiki_sorted:
        language = s[0]
        count = s[1]
        percentage = (count / total_petitions) * 100
        out = language + "\t(" + str(count) + "," + str(percentage) + "%)" + "\n"
        output_stream.write(out)

    pass

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(debug, i_file_name, total_petitions, o_file_name):
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
    my_reduce(my_input_stream, total_petitions, my_output_stream)

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



    # This variable must be computed in the first stage
    total_petitions = 21996787

    i_file_name = "sort_simulation.txt"
    o_file_name = "reduce_simulation.txt"

    my_main(debug, i_file_name, total_petitions, o_file_name)
