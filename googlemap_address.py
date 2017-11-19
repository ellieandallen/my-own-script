'''
Created on 2017/07/12

@author: ellie
'''

import googlemaps
import json
import numpy as np
from datetime import datetime
import unicodedata

# gmaps = googlemaps.Client(key='***AIzaSyBI_7HyBMlyoQLuaVdnIeMLCWyt0swm6ik')
# gmaps = googlemaps.Client(key='***AIzaSyAWKocF8vFCAlrslDeb3HZ2hPLt4pPEA_Q')


def get_gps(address):
    reverse_geocode_result = gmaps.geocode(address)
    gps_list = []
    l = len(reverse_geocode_result)
    if reverse_geocode_result is None or l == 0:
        gps_list = ['invalid address']
    else:
        for num in range(0, l):
            gps_list.append((reverse_geocode_result[num]['geometry']['location']['lat'],
                             reverse_geocode_result[num]['geometry']['location']['lng']))
    return gps_list


if __name__ == '__main__':
    with open('/home/ellie/oppoaddress.csv', 'r') as f:
        data = f.readlines()
    data = data[:5]
    for i in range(1, len(data)):
        # a = unicodedata.normalize('NFKC', data[i])
        data[i] = [data[i], get_gps(data[i])]
    # data = ['address\n', ['Indonesia,Jawa Barat,Bekasi,warung bongkok\n', [(-6.320813999999999, 107.100693),
    # (-6.277625, 107.1147013)]], ['Indonesia,DKI Jakarta,Jakarta Timur,Tamini Square Lt 2 Block ss 38 No.1&2\n', []],
    #  ['Indonesia,DKI Jakarta,Jakarta Timur,Tamini Square LT 2 Block ss 33 No.3-5\n', []], ['Indonesia,DKI Jakarta,
    # Jakarta Timur,Tamini Square LT 2 Block ss 33 No 6&7\n', []]]
    data = json.dumps(data)
    with open('/home/ellie/oppoaddress_final.csv', 'w') as f:
        f.write(data)

    # with open('/home/ellie/oppoaddress_final.csv', 'r') as f:
    #     data = f.read()
    # data = json.loads(data)
    # print data
    print 'finished'
