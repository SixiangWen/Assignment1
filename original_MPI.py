import json
import re
import time
import original_codes
import argparse
from collections import Counter
from mpi4py import MPI

# mpiexec -n 1 python -m mpi4py MPI.py
# mpiexec -n 8 python -m mpi4py MPI.py
# scp /Users/wsx/Desktop/UniMelb/CCC/Assignment1/one_node_eight_cores.slurm sixiang@spartan.hpc.unimelb.edu.au:/home/sixiang
# cd /data/projects/COMP90024/
# only the tweets that have location are processed

def main(file_path2,file_path3):

    start_time = time.time()
    comm = MPI.COMM_WORLD
    comm_rank = comm.Get_rank()
    comm_size = comm.Get_size()

    #  tweet['doc']['coordinates']
    cells_count = Counter()
    # tweet['doc']['metadata']['iso_language_code']
    lan_count_per_cell = {}
    # for i in ['A1', 'A2', 'A3', 'A4', 'B1', 'B2', 'B3', 'B4', 'C1', 'C2', 'C3', 'C4', 'D1', 'D2', 'D3', 'D4']:
    #     lan_count_per_cell[i] = Counter()

    map = original_codes.read_cells(file_path3)

    number_of_lines = original_codes.get_number_of_lines(file_path2)

    # every processor handle about block_size tweets
    block_size = number_of_lines//comm_size # 1249
    start_line = comm_rank*block_size
    # the last core should deal with all of the rest tweets
    if number_of_lines % comm_size != 0:
        end_line = number_of_lines+1 if comm_rank == comm_size-1 else start_line + block_size
    else:
        end_line = number_of_lines if comm_rank == comm_size-1 else start_line + block_size



    with open(file_path2, 'r', encoding='utf-8') as fp:
        a = 0
        # printout the lines from 11 to 20
        # 10 lines in total
        # [10,20]
        for tweet in fp:
            # to skip the first line and all the lines before the core should process
            if a == 0 or a < start_line:
                a += 1
                continue
            # where each core process the allocated tweets
            elif a < end_line:
                # three cases of different ending strings
                if tweet.endswith(',\n'):
                    line = json.loads(tweet.rstrip(',\n'))
                elif tweet.endswith('}}\n'):
                    line = json.loads(tweet[:-1])
                elif tweet.endswith(']}\n'):
                    line = json.loads(tweet[:-2])
                else:
                    print('There is another case of ending string!')
                # if the geo info is available, move on, otherwise continue the loop
                if line['doc']['coordinates']:
                    if original_codes.map_to_cell(line,map):
                        cells_count[original_codes.map_to_cell(line, map)] += 1
                    original_codes.count_language_per_cell(lan_count_per_cell, line, map)
                    a += 1
                else:
                    a += 1
                    continue
            else:
                break


    # 0.02 sec


    if comm_size==1:
        final_cells_count = cells_count
        list_of_dic = [lan_count_per_cell]

    else:
        # if there are multiple cores existing, do a simple merging operation across all of these cores
        final_cells_count = comm.reduce(cells_count, op=MPI.SUM, root=0)
        list_of_dic = comm.gather(lan_count_per_cell, root=0)



    if comm_rank==0:
        final_lan_count_per_cells = {}
        print('*****'*5,'Results are as below','*****'*5)
        for i in list_of_dic:
            for a, b in i.items():
                if a not in final_lan_count_per_cells.keys():
                    final_lan_count_per_cells[a] = b
                else:
                    final_lan_count_per_cells[a] += b
                    pass
            pass

        original_codes.pretty_print(final_cells_count,final_lan_count_per_cells)
        time_spent = time.time() - start_time
        print("Programs runs {}(s)".format(time_spent))
        pass


if __name__ == "__main__":
    # # Instantiate the parser
    # parser = argparse.ArgumentParser(description='python to process data')
    # # Required country code file
    # parser.add_argument('-language', type=str, help='A required string path to language code file')
    # # Required geo data path
    # parser.add_argument('-data', type=str, help='A required string path to data file')
    # #
    # parser.add_argument('-coordinates',type=str,help='A file path to the grid data')
    # args = parser.parse_args()
    #
    # lan = args.lan
    # data = args.data
    # coor = args.coor


    main('smallTwitter.json','sydGrid-2.json')


    # 测试几种特殊情况下的数据集
    # 1.如果tweet位置刚好在边界上
    # 2.数据集中推特的数量可以被整除
    # 3.某一条推特有坐标，但是坐标不在讨论范围内
    # 4.有多个cell中都存在tweet，且每个cell中的tweets都有不同的语言
    # 在不同的行数尝试，即不同的core
    # A1中有5条数据,3条中文，2条法语
    # B2中有6条数据，3条日语，2条西语，1条俄语
    # D4中有4条数据...