import os
import requests
import time, datetime
import xmltodict

api_key = 'MW9S-E7SL-26DU-VV8V'
url = 'http://api.bart.gov/api/sched.aspx?cmd=routesched'

dates = ['wd', 'sa', 'su']

routes = [1, 2, 3, 4, 5, 6, 7, 8, 11, 12, 19, 20]
routes = [str(i) for i in routes]

scheds = []

for d in dates:
	for r in routes:
		response = requests.get(url + '&key=' + api_key + '&route=' + r + '&date=' + d)
		print url + '&key=' + api_key + '&route=' + r + '&date=' + d
		raw = xmltodict.parse(response.text)
		sched_num = raw['root']['sched_num']
		try:
			trains = raw['root']['route']['train']
			for train in trains:
				index = train['@index']
				stops = train['stop']
				for stop in stops:
					station = stop['@station']
					try:
						origTime = stop['@origTime']
					except:
						origTime = ''
					bikeflag = stop['@bikeflag']

					scheds.append([sched_num, d, r, index, station, origTime, bikeflag])


		except:
			scheds.append([sched_num, d, r, 'NULL', 'NULL', 'NULL', 'NULL'])


with open('routesched.txt', 'w') as writefile:
	for s in scheds:
		writefile.writelines("\t".join(v for v in s)+"\n")
