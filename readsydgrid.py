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