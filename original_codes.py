import json
import re
import time
from collections import Counter


#  tweet['doc']['coordinates']
cells_count = Counter()
# tweet['doc']['metadata']['iso_language_code']
lan_count_per_cell = {}

def merge_dicts(dict_args):
    #merge several dictionary
    result = {}
    for dictionary in dict_args:
        result.update(dictionary)
    return result

def readsydgrid(filepath):
    import json
    lr=['A1','B1','C1','D1','A2','B2','C2','D2','A3','B3','C3','D3','A4','B4','C4','D4']
    with open('sydGrid.json','r') as fp:
        data = json.load(fp)
    fp.close()
    array=[]
    a=0
    x=[]
    y=[]
    for i in data['features']:
        for c in i['geometry']['coordinates']:
            if a == 0:
                sub = [[[0]*2]*4]*4
                array.insert(0,sub)
                array[0][0] = c[:4]
                x.append(c[0][0])
                y.append(c[0][1])
                a+=1
            else:
                if c[0][0] not in x:
                    x.append(c[0][0])
                    x.sort()
                    locx = x.index(c[0][0])
                    sub = [[[0]*2]*4]*4
                    array.insert(locx*4,sub)
                    if c[0][1] not in y:
                        y.append(c[0][1])
                        y.sort(reverse=True)
                        locy = y.index(c[0][1])
                        if locy<len(y)-1:
                            for l in range(len(x)):
                                array[l][locy+1:] = array[l][locy:4-locy-1]

                        array[locx][locy] = c[:4]
                    else:
                        locy = y.index(c[0][1])
                        array[locx][locy] = c[:4]

                else:
                    locx = x.index(c[0][0])
                    if c[0][1] not in y:
                        y.append(c[0][1])
                        y.sort(reverse=True)
                        locy = y.index(c[0][1])
                        if locy<len(y)-1:
                            for l in range(len(x)):
                                array[l][locy+1:] = array[l][locy:4-locy-1]

                        array[locx][locy] = c[:4]
                    else:
                        locy = y.index(c[0][1])
                        array[locx][locy] = c[:4]

                a+=1
    map = merge_dicts([dict(zip(lr, array[0])),dict(zip(lr[4:], array[1])),dict(zip(lr[8:], array[2])),dict(zip(lr[12:], array[3]))])
    return map

def get_number_of_lines(file_path):
    with open(file_path,'r',encoding='utf-8') as f:
        n = 0
        for i in f:
            if i :
                n+=1
            else:
                break
        return n

def read_cells(file_path):
    map = {}
    # lr = ['A1', 'B1', 'C1', 'D1', 'A2', 'B2', 'C2', 'D2', 'A3', 'B3', 'C3', 'D3', 'A4', 'B4', 'C4', 'D4']
    lr = ['C4', 'B4', 'A4', 'D3', 'C3', 'B3', 'A3', 'D2', 'C2', 'B2', 'A2', 'D1', 'C1', 'B1', 'A1', 'D4']
    with open(file_path, 'r') as fp:
        data = json.load(fp)
        a = 0
        for i in data['features']:
            for c in i['geometry']['coordinates']:
                map[lr[a]] = c[:4]
                a += 1
    return sorted(map.items())


# def read_lan_code(file_path):
#     pass


def map_to_cell(tweet,map):
    """
    :param tweet: certain tweet in the dataset
    :return: the corresponding cell that tweet belongs to
    """
    # # read in the sydGrid file as a dict
    # map={}
    # lr=['A1','B1','C1','D1','A2','B2','C2','D2','A3','B3','C3','D3','A4','B4','C4','D4']
    # with open('sydGrid.json','r') as fp:
    #     data = json.load(fp)
    #     a = 0
    #     for i in data['features']:
    #         for c in i['geometry']['coordinates']:
    #             map[lr[a]]=c[:4]
    #             a+=1
    #             # print(c[:4])
    #
    # # print(map)

    # if it does exist
    # if tweet['doc']['coordinates']:
    #     x_coordinate = float(tweet['doc']['coordinates']['coordinates'][0])
    #     y_coordinate = float(tweet['doc']['coordinates']['coordinates'][1])
    # else:
    #     return
    x_coordinate = float(tweet['doc']['coordinates']['coordinates'][0])
    y_coordinate = float(tweet['doc']['coordinates']['coordinates'][1])


    for k,v in map:

        # print(k,v)

        if x_coordinate>=float(v[0][0]) and x_coordinate<=float(v[2][0]):
            if y_coordinate>float(v[1][1]) and y_coordinate<=float(v[0][1]):
                # the tweet is in cell k
                return k
        else:
            continue


def count_language_per_cell(dict,tweet,map):
    with open('language.json','r') as fp:
        lan_code = json.load(fp)

    # if tweet['doc']['coordinates']:
    #     x_coordinate = float(tweet['doc']['coordinates']['coordinates'][0])
    #     y_coordinate = float(tweet['doc']['coordinates']['coordinates'][1])
    # else:
    #     return
    x_coordinate = float(tweet['doc']['coordinates']['coordinates'][0])
    y_coordinate = float(tweet['doc']['coordinates']['coordinates'][1])

    for k,v in map:

        if (x_coordinate>=float(v[0][0]) and x_coordinate<=float(v[2][0])):
            if (y_coordinate>float(v[1][1]) and y_coordinate<=float(v[0][1])):
                # print('test')
                # the tweet is in cell k
                if k not in dict.keys():
                    dict[k] = Counter()
                dict[k][lan_code[tweet['doc']['metadata']['iso_language_code']]] += 1
                # print('test')
                break
        else:
            continue

def pretty_print(c_cou,l_cou):
    print('Cell',' ','#Total Tweets',' ','#Number of language used',' ','#Top 10 languages')
    for k,v in sorted(c_cou.items()):
        print(k,' '*5,v,' ',' ',len(l_cou[k]),' ',l_cou[k].most_common(10))
        l = ''
        for a,b in l_cou[k].most_common(10):
            pass
    pass

# def language_code(tweet):
#     with open('language.json','r') as fp:
#         lan_code = json.load(fp)
#     # return the corresponding language
#     # only return the language of the tweets that have coordinates available
#         return lan_code[tweet['doc']['metadata']['iso_language_code']]


    # print(lan_code)




def read_tweets(file_path):

    map = read_cells('sydGrid-2.json')

    with open(file_path,'r') as fp:
        # define how many tweets you want to read in

        a=0
        # number of tweets that got read in

        # while True:
        #    if not line:
        #        break

        while a<4999:
            if a==0:
                # to skip the first line
                # and get the total number of tweets in the file
                number_of_rows = int(re.findall("\d+", fp.readline().strip())[0])-1
                print('There are {} tweets in this dataset.'.format(number_of_rows))
                a+=1
            else:
                line = json.loads(fp.readline().rstrip(',\n'))
                # if it exists then print out the coordinates
                if line['doc']['coordinates']:
                    if map_to_cell(line,map):
                        cells_count[map_to_cell(line,map)]+=1
                    print(line['doc']['coordinates']['coordinates'])
                    count_language_per_cell(lan_count_per_cell,line,map)
                    # lan_count[language_code(line)] += 1
                else:
                    a+=1
                    continue
                    # print(language_code(line))
                    # print('the tweet was made in language {}\nand the id was {} with number {}'.
                    #       format(line['doc']['metadata']['iso_language_code'],line['id'],a+1))
                    # print("no such attribute found")
                a+=1

    # print(lan_count_per_cell,'\n',cells_count)
    return

# def seek_and_tell():
#     with open('tinyTwitter.json','r') as f:
#         f.seek(0)
#         print(f.readline().rstrip('\n'))
#         print(f.tell())
#
#         # f.readline()
#         # a = f.tell()
#         # f.seek(a)
#         # tweet1 = json.loads(f.readline().rstrip(',\n'))
#         # print(tweet1['doc']['metadata']['iso_language_code'])
#         # b = f.tell()
#         # f.seek(b)
#         # tweet2 = json.loads(f.readline().rstrip(',\n'))
#         # print(tweet2['doc']['metadata']['iso_language_code'])
#         # f.readline()
#         # print(f.tell())


def main():
    # 0.2 sec
    # start_time = time.time()
    # read_tweets('smallTwitter.json')
    # print(cells_count, '\n', lan_count_per_cell)
    # time_spent = time.time()-start_time
    # print("Programs runs {}(s)".format(time_spent))
    print(read_cells('sydGrid.json'))
    print(readsydgrid('sydGrid.json'))
    # "geo":{"type":"Point","coordinates":[-33.86,151.211]},"coordinates":{"type":"Point","coordinates":[151.211,-33.86]},


    # print(read_cells('sydGrid-2.json'))

    # str1 = '{dsadsabfwuq}},\n'
    # str2 = '{dsadsa}}]}\n'
    # str3 = '{dsacxndq}}\n'
    # str4 = str1.rstrip(']},\n')+'}'
    # print(str1.rstrip(']},\n')+'}')
    # print(str2.rstrip(']},\n'))
    # print(str3.rstrip(']},\n'))
    # print(str4)


    # dict
    # print(language_code('language.json'))


    # test_co=[151.20747, -33.8705799]
    # print(map_to_cell(test_co))


    # seek_and_tell()

    # print(len('{"total_rows":1000,"rows":['))

    # testdic = {}
    # testdic['A1'] = Counter()
    # for i in 'aaabbcbdedde':
    #     testdic['A1'][i]+=1
    #
    # print(testdic)

    # # Counter can be added up directly
    # tc2 = Counter('aaaaabbbsssdddwww')
    # tc3 = Counter('dddaaiiiwwooxxppsss')
    # print(tc2+tc3)


    # time_start1 = time.time()
    # with open('smallTwitter.json','r',encoding='utf-8') as fp:
    #     a = 0
    #     # printout the lines from 11 to 20
    #     # 10 lines in total
    #     # [10,20]
    #     for line in fp:
    #         if a == 0 or a<4999:
    #             a+=1
    #             continue
    #         elif a<5000:
    #             if line.endswith(',\n'):
    #                 print(json.loads(line.rstrip(',\n')))
    #             elif line.endswith('\n'):
    #                 print(json.loads(line[:-3]))
    #             pass
    #             a+=1
    #         else:
    #             break
    # time_spent1 = time.time()-time_start1
    # print(time_spent1)

    # time_start2 = time.time()
    # with open('smallTwitter.json', 'r', encoding='utf-8') as fp:
    #     lines = fp.readlines()
    #     a = 0
    #     # printout the lines from 11 to 20
    #     # 10 lines in total
    #     # [10,20]
    #     for line in lines:
    #         if a == 0 or a < 990:
    #             a += 1
    #             continue
    #         elif a < 992:
    #             print(line.rstrip(',\n'))
    #             pass
    #             a += 1
    #         else:
    #             break
    # time_spent2 = time.time() - time_start2
    # print(time_spent2)

            # print(line)

    pass






if __name__=='__main__':
    main()





