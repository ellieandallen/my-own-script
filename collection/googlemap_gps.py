import googlemaps
import json
import pandas as pd
import numpy as np
from datetime import datetime

# gmaps = googlemaps.Client(key='***AIzaSyBI_7HyBMlyoQLuaVdnIeMLCWyt0swm6ik')
# gmaps = googlemaps.Client(key='***AIzaSyAWKocF8vFCAlrslDeb3HZ2hPLt4pPEA_Q')


def get_address(latitude, longitude):
    reverse_geocode_result = gmaps.reverse_geocode((latitude, longitude))
    if reverse_geocode_result:
        most_accurate_location = json.dumps(reverse_geocode_result[0]['formatted_address'])
        a = most_accurate_location.split(',')
        l = len(a)
        return a, l
    else:
        print latitude, longitude
        return """invalid_address""", 1

df = pd.read_csv('/home/ellie/location.csv', encoding='utf8', index_col=False)
# df = df[:5]
print df[:5]

df['val_1'] = df['val_1'].astype(str).str.replace('[', '').str.replace(']', '').str.split(',')
print df['val_1']
df['val_2'] = 0
df['val_3'] = 0
df['val_4'] = 0
df['val_5'] = 0

for i in range(0, len(df['val_1'])):
    temp, l = get_address(df['val_1'][i][0], df['val_1'][i][1])
    if l == 6:
        df['val_2'][i] = temp[4]
        df['val_3'][i] = temp[3]
        df['val_4'][i] = temp[2]
        df['val_5'][i] = temp[1] + temp[0]
    elif l == 5:
        df['val_2'][i] = temp[3]
        df['val_3'][i] = temp[2]
        df['val_4'][i] = temp[1]
        df['val_5'][i] = temp[0]
    elif l == 4:
        df['val_2'][i] = temp[2]
        df['val_3'][i] = temp[1]
        df['val_4'][i] = temp[0]
    elif l == 7:
        df['val_2'][i] = temp[5]
        df['val_3'][i] = temp[4]
        df['val_4'][i] = temp[3]
        df['val_5'][i] = temp[2] + temp[1] + temp[0]
    elif l == 8:
        df['val_2'][i] = temp[6]
        df['val_3'][i] = temp[5]
        df['val_4'][i] = temp[4]
        df['val_5'][i] = temp[3] + temp[2] + temp[1] + temp[0]
    else:
        df['val_5'][i] = temp

df.to_csv('/home/ellie/location_final.csv', encoding='utf8')
print 'finished'

