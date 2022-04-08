import json
from collections import Counter


def get_number_of_lines(file_path):
    """

    :param file_path: the file path for bigTwitter.json
    :return: the number of lines in the file
    """
    with open(file_path,'r',encoding='utf-8') as f:
        n = 0
        # simple for loop to count the number of lines in the file
        for i in f:
            if i:
                n+=1
            else:
                break
        return n

def read_cells(file_path):
    """

    :param file_path: the file path for sydGrid.json
    :return: a list of tuples of the cell name and the coordinates for each cell
    """
    cells = {}
    # since the id in the sydGrid.json is chaotic, manually set up a list of name for corresponding cell
    lr = ['C4', 'B4', 'A4', 'D3', 'C3', 'B3', 'A3', 'D2', 'C2', 'B2', 'A2', 'D1', 'C1', 'B1', 'A1', 'D4']
    with open(file_path, 'r') as fp:
        data = json.load(fp)
        a = 0
        for i in data['features']:
            for c in i['geometry']['coordinates']:
                cells[lr[a]] = c[:4]
                a += 1

    return sorted(cells.items()) # sort the dict by its key values to transform it into the order of A1,A2,A3...



def map_to_cell(tweet,cells):
    """

    :param tweet: certain single tweet in the dataset
    :param cells: the output of read_cells function
    :return: the corresponding cell name that tweet belongs to
    """

    #  just for convenience, x_coordinate is longitude and y_coordinate is latitude
    x_coordinate = float(tweet['doc']['coordinates']['coordinates'][0])
    y_coordinate = float(tweet['doc']['coordinates']['coordinates'][1])

    # seek to which cell the tweet belongs
    # if none is satisfied, return None
    for k,v in cells:

        if x_coordinate>=float(v[0][0]) and x_coordinate<=float(v[2][0]):
            if y_coordinate>float(v[1][1]) and y_coordinate<=float(v[0][1]):
                # the tweet belongs to cell k
                return k
        else:
            continue
    return


def count_language_per_cell(dict,tweet,cells):
    """
    Very similar to the function map_to_cell
    :param dict: a dict that stores the language counting information
    :param tweet
    :param cells
    """
    # the language code json file, in the form of a dict
    with open('language.json','r') as fp:
        lan_code = json.load(fp)

    x_coordinate = float(tweet['doc']['coordinates']['coordinates'][0])
    y_coordinate = float(tweet['doc']['coordinates']['coordinates'][1])


    for k,v in cells:

        if (x_coordinate>=float(v[0][0]) and x_coordinate<=float(v[2][0])):
            if (y_coordinate>float(v[1][1]) and y_coordinate<=float(v[0][1])):
                # the tweet belongs to cell k
                # if it is not in dict, create a Counter for it
                if k not in dict.keys():
                    dict[k] = Counter()
                # map the code to corresponding language and count it
                dict[k][lan_code[tweet['doc']['metadata']['iso_language_code']]] += 1
                break
        else:
            continue

def pretty_print(c_cou,l_cou):
    """
    Take the final results after processing, and make it look better in printing
    :param c_cou: the cells counter
    :param l_cou: the language counter
    :return:
    """
    print('*****' * 5, 'Results are as below', '*****' * 5)
    f1 = '{0:8}\t{1:8}\t{2:10}'
    print(f1.format('Cell','#Total Tweets','#Number of language used'))
    for k,v in sorted(c_cou.items()):
        print(f1.format(k,v,len(l_cou[k])))
    f2 = '{0:8}\t{1:8}'
    print(f2.format('Cell','#Top 10 languages'))
    for k,v in sorted(l_cou.items()):
        print(k,'            ',v.most_common(10))








