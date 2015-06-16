#test reading weather json files
import simplejson
from pprint import pprint

with open('data/weather_test.txt', 'r') as jsonfile:
	content = jsonfile.read().split('\n\n')
	for c in content[:-1]:
	    print 'c'
	    raw = simplejson.loads(c)
	    print 'r'
	    pprint(raw)