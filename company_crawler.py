'''
Created on 2017/06/26

@author: ellie
'''

# -*- coding:utf-8 -*-
import pandas as pd
import urllib2
import urllib
from bs4 import BeautifulSoup


def search_keyword(word):
    # company_list = []
    try:
        url = "https://www.ahu.go.id/pencarian/bakum/cari/tipe/perseroan?nama={}".format(urllib.pathname2url(word))
        request = urllib2.Request(url)
        response = urllib2.urlopen(request)
        html_doc = response.read()
        soup = BeautifulSoup(html_doc, 'html.parser', from_encoding='utf-8')
        for el in soup.find_all('strong'):
            # print el.get_text()
            # company_list.append(el.get_text())
            with open('/home/ellie/result.csv', 'w') as f:
                f.write(el.get_text())

    except:
        print 'exception'
    # return company_list


# get the first match name
# def search_keyword(word):
#     # company_list = []
#     try:
#         url = "https://www.ahu.go.id/pencarian/bakum/cari/tipe/perseroan?nama={}".format(urllib.pathname2url(word))
#         request = urllib2.Request(url)
#         response = urllib2.urlopen(request)
#         html_doc = response.read()
#         soup = BeautifulSoup(html_doc, 'html.parser', from_encoding='utf-8')
#         print soup.strong.get_text()
#
#     except:
#         print 'exception'
#     # return company_list

df = pd.read_csv('/home/ellie/company.csv', encoding='utf8', header=None, names=['cname'])
df = df[:5]
df['result'] = df.ix[:, 0].apply(search_keyword)
# df.to_csv('/home/ellie/company.csv', encoding='utf8')\
print 'finished'
