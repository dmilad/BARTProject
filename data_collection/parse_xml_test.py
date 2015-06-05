import xmltodict
import datetime


with open('data/rt_etd_2015-06-03.txt', 'r') as readfile:
    content = readfile.read().split('\n\n')
    for c in content[:-1]:
        print 'c'
        raw = xmltodict.parse(c)
        print 'r'
        #print raw
