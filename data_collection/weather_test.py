import os, sys
import pyowm
import time, datetime
from owm_cred import owm_api_key
from pprint import pprint
import simplejson
current = True
owm = pyowm.OWM('owm_api_key')

latlongs = 'weather_latlongs_sample.txt'
with open(latlongs, 'r') as readfile:

	#record hour of the fetch (to use in file name)
	fetchtime = datetime.datetime.today()
	fetchhour = fetchtime.hour
	#record today's daye (to use in file name)
	today = datetime.date.today()

	for line in readfile:
		loc, lat, lon = line.split(': ')[0], float(line.split(': ')[1].split(', ')[0]), float(line.split(': ')[1].split(', ')[1])
		#current weather data

		#weat = owm.weather_at_coords(lat, lon).to_JSON()
		weat = owm.three_hours_forecast_at_coords(lat, lon).get_forecast().to_JSON()

		print
		pprint(simplejson.loads(weat))

