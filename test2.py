import json
import re
from collections import Counter

tweets_num_cells = Counter()
language_used = Counter()

# def read_data_line_by_line(file_path: str):
#     with open(file_path, 'r', encoding='utf-8') as file:
#         for line in file:
#             yield line
# for line in read_data_line_by_line('tinyTwitter 2.json'):
#     print(line)

# with open("tinyTwitter.json",'r') as tf:
#     for line in tf.readlines():
#         print(line)

# with open("tinyTwitter.json",'r') as load_f:
#     load_dict = json.load(load_f)
#
# a=0
# for temp in load_dict['rows']:
#     a+=1
#     print(temp)
#     # print(temp['doc']['coordinates'])
# print(a)

# d_p='sydGrid.json'
#
# def tweet_to_region(data_path):
#
#     f = open(data_path, "r")
#     melbGrid = json.loads(f.read(data_path))
#     f.close()
#     melbGrid

def read_tweets(file_path):
    with open(file_path,'r') as fp:
        # define how many tweets you want to read in
        a=0
        while a<10:
            if a==0:
                # to skip the first line
                # and get the total number of tweets in the file
                number_of_rows = int(re.findall("\d+", fp.readline().strip())[0])-1
                print(number_of_rows)
                a+=1
            else:
                line = json.loads(fp.readline().rstrip(',\n'))
                if line['doc']['coordinates']:
                    print(line['doc']['coordinates'])

                else:
                    print('the tweet was made in language {}\nand the id was {} with number {}'.
                          format(line['doc']['metadata']['iso_language_code'],line['id'],a+1))
                    print("no such attribute found")
                a+=1
        return


def read_code(file_path):
    with open(file_path,'r') as fp:
        return json.load(fp)


def main():
    # dict
    # print(read_code('language.json'))
    # read_tweets('tinyTwitter.json')
    test_cou = Counter({'C1':10,'A2':20,'B4':30})
    test_cou2 = Counter({'C1':Counter({'English': 15,'French':20}),'A2':Counter({'English': 15}),'B4':Counter({'English': 15})})
    test_dic = {'a':Counter()}
    for k,v in sorted(test_cou2.items()):
        print(k,len(v))





if __name__=='__main__':
    main()
