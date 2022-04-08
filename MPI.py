import json
import time
import util
from collections import Counter
from mpi4py import MPI

# import argparse

def main(file_path1,file_path2):
    """
    :param file_path1: the file path for bigTwitter.json
    :param file_path2: the file path for sydGrid.json
    """

    start_time = time.time() # start time

    # some default setting for MPI programming
    comm = MPI.COMM_WORLD
    comm_rank = comm.Get_rank()
    comm_size = comm.Get_size()

    # to store the results
    cells_count = Counter() # a counter for cells counting
    lan_count_per_cell = {} # a dict of counters for language counting


    cells = comm.bcast(util.read_cells(file_path2), root=0) # broadcast the cells coordinates to every core

    # since the first line and the last line of bigTwitter.json are both non-significant
    # the actual number of tweets should be -2
    number_of_tweets = comm.bcast(util.get_number_of_lines(file_path1) - 2, root=0)


    block_size = number_of_tweets//comm_size # every processor handle about block_size tweets
    start_line = comm_rank*block_size # the number of line that certain core should start from

    # end_line is the number of line where the core should stop
    # and the last core must handle rest of the tweets
    if number_of_tweets % comm_size != 0:
        end_line = number_of_tweets+1 if comm_rank == comm_size-1 else start_line + block_size
    else:
        end_line = number_of_tweets if comm_rank == comm_size-1 else start_line + block_size



    with open(file_path1, 'r', encoding='utf-8') as fp:

        a = 0

        for line in fp:
            # to skip the first line and all the lines before the core start processing
            if a == 0 or a < start_line:
                a += 1
                continue

            # each core process the allocated tweets
            elif a < end_line:

                # handle different endings of line
                # strip all the endings and plus '}}' to make sure it can be loaded as a json form
                tweet = json.loads(line.rstrip(']},\n')+'}}')

                # if the geo info is available and language is not undefined, move on, otherwise continue the loop
                if tweet['doc']['coordinates'] and tweet['doc']['metadata']['iso_language_code'] !='und':
                    # if it is in a certain cell, move on, otherwise continue the loop
                    if util.map_to_cell(tweet, cells):
                        # cells counting
                        cells_count[util.map_to_cell(tweet, cells)] += 1
                    else:
                        a += 1
                        continue
                    # language counting
                    util.count_language_per_cell(lan_count_per_cell, tweet, cells)
                    a += 1
                else:
                    a += 1
                    continue
            else:
                break



    # one core
    if comm_size == 1:
        final_cells_count = cells_count
        list_of_dic = [lan_count_per_cell]

    # multiple cores
    else:
        # a simple merging operation across all of these cores
        final_cells_count = comm.reduce(cells_count, op=MPI.SUM, root=0) # can be summed by reduce directly
        list_of_dic = comm.gather(lan_count_per_cell, root=0) # have to be further processed in the master core


    # master core gathers all the results and print it out
    if comm_rank==0:
        final_lan_count_per_cells = {}
        # merge the language counting information
        for i in list_of_dic:
            for k, v in i.items():
                if k not in final_lan_count_per_cells.keys():
                    final_lan_count_per_cells[k] = v
                else:
                    final_lan_count_per_cells[k] += v
                    pass
            pass

        # print out the result in a better form
        # print(final_lan_count_per_cells)
        util.pretty_print(final_cells_count, final_lan_count_per_cells)
        time_spent = time.time() - start_time # time used
        print("Programs runs {}(s)".format(time_spent))
        pass


if __name__ == "__main__":

    # main('/data/projects/COMP90024/bigTwitter.json','/data/projects/COMP90024/sydGrid.json')
    main('smallTwitter.json', 'sydGrid.json')

