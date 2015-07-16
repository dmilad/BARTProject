import os
import requests
import time, datetime
import xmltodict

api_key = 'MW9S-E7SL-26DU-VV8V'
url = 'http://api.bart.gov/api/sched.aspx?cmd=load'
ssss = ['SHAY', 'FTVL', 'NCON', 'COLS', 'WCRK', 'WOAK', 'CONC', 'CAST', 'ORIN', 'SFIA', 'WDUB', 'DALY', 'EMBR', 'CIVC', 'SBRN', 'HAYW', 'LAFY', 'DBRK', '12TH', 'PHIL', 'PITT', 'NBRK', 'MONT', 'POWL', 'ROCK', 'SANL', '16TH', 'FRMT', 'BALB', 'DELN', 'MCAR', 'DUBL', '24TH', 'PLZA', 'MLBR', 'GLEN', 'BAYF', 'LAKE', 'ASHB', 'UCTY', '19TH', 'COLM', 'RICH', 'SSAN', 'OAKL']

rr = [1, 2, 3, 4, 5, 6, 7, 8, 11, 12, 19, 20]
rr = [str(i) for i in rr]
for i, r in enumerate(rr):
	if len(r) == 1:
		rr[i] = '0'+r
print rr
tt = ['0'+str(i) for i in range(1, 10)]
tt += [str(i) for i in range(10, 100)]

loads = []

for a in ssss:
	for b in rr:
		for c in tt:
			ld = a + b + c
			response = requests.get(url + '&key=' + api_key + '&ld1=' + ld)
			raw = xmltodict.parse(response.text)
			req = raw['root']['load']['request']
			sched_id = req['@scheduleID']
			leg = req['leg']
			load = leg['@load']
			print [sched_id, a, b, c, load]
			if int(load) > 0:
				print 'yes!'
				loads.append([sched_id, a, b, c, load])

with open('load.txt', 'a') as writefile:
	writefile.write(loads)

print loads
