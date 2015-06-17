#call with one argument: 'current' for current weather, 'forecast' for forecast weather. for ex:
#python weather_fetcher.py current

import os, sys
import pyowm
import time, datetime
from owm_cred import owm_api_key
from pprint import pprint
import object_storage
import simplejson

def fetch(latlongs, current = True):

	"""
	generate OpenWeatherMap API commands, fetch the response for each, and write to appropriate text files. 
	"""

	owm = pyowm.OWM('owm_api_key')

	with open(latlongs, 'r') as readfile:

		#record hour of the fetch (to use in file name)
		fetchtime = datetime.datetime.today()
		fetchhour = fetchtime.hour
		#record today's daye (to use in file name)
		today = datetime.date.today()

		for line in readfile:
			loc, lat, lon = line.split(': ')[0], float(line.split(': ')[1].split(', ')[0]), float(line.split(': ')[1].split(', ')[1])

			#current weather data
			if current:
				weat = owm.weather_at_coords(lat, lon).to_JSON()
				with open('data/' + loc + '_current_'+ str(today) + '_' + str(fetchhour) +'.txt', 'a') as writefile:
					writefile.write(weat + '\n\n')
				print str(fetchtime), loc + ' current'

			#forecast data
			else:
				weat = owm.three_hours_forecast_at_coords(lat, lon).get_forecast().to_JSON()
				with open('data/' + loc + '_forecast_'+ str(today) + '_' + str(fetchhour) +'.txt', 'a') as writefile:
					writefile.write(weat + '\n\n')
				print str(fetchtime), loc + ' forecast'

		return fetchtime



def send_to_storage(maindir = 'data'):

	"""
	sends 
	"""
	here = [f for f in os.walk(maindir).next()[2] if f.endswith(".txt")]
	#print here

	move = list(set(here))
	#print move

	#send here
	for filetosend in here:
		fullfilename = 'data/' + filetosend
		with open(fullfilename, 'r') as readfile:
			print "Sending " + fullfilename
			sl_storage['weather_dump'][readfile.name.split('/')[-1]].send(readfile)
	print 'All data files sent to object store.'

	#move move to sentdata
	for filetomove in move:
		os.rename(maindir+'/'+filetomove, maindir+'/sentdata/'+filetomove)
	print "Last hour's data moved."



def start(current = True):
	sent = False
	while True:
		#used to time 30 seconds between requests
		start_time = time.time()
		fetchtime = datetime.datetime.today()

		#in the first minute of every hour, send files to object storage. 
		#the purpose of the "sent" boolean to make sure we dont send same files twice in the first minute
		try:
			if fetchtime.minute == 0 and not sent:
				send_to_storage()
				sent = True
			else:
				sent = False
		except Exception, e:
			print "An error occurred. Files could not be sent to object store at " + str(fetchtime), e

		#time 30 seconds between requests
		end_time = time.time()
		duration = end_time - start_time 
		time.sleep(max([0, 30 - duration]))

		#call fetch, which makes api calls based on the locations provided in the latlongs files
		#record the fetch time in the appropriate file, constructed using the date of fetch (one fetchfile per date)
		try:
			fetchtime = fetch('weather_latlongs.txt', current)
		except:
			pass


if __name__ == "__main__":

	if sys.argv[1] == 'current':
		#run this forever
		while True:
			#if there is an error, try again in 60 seconds
			try:
				start(current = True)
			except Exception, e:
				print "Error: ", e
				print "Retrying in 60 seconds..."
			time.sleep(60)

	else:
		#run this forever
		while True:
			#if there is an error, try again in 60 seconds
			try:
				start(current = False)
			except Exception, e:
				print "Error: ", e
				print "Retrying in 60 seconds..."
			time.sleep(60)


