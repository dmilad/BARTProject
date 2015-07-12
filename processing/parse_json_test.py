import simplejson
from pprint import pprint
import datetime
#import MySQLdb
from mysql_cred import mysql_user, mysql_passowrd, mysql_database
import object_storage
import re
import os
import time
from swift_cred import sl_user_name, sl_api_key, sl_data_center

def weather_pipeline(col_names, values):
	#create storage client object
	sl_storage = object_storage.get_client(sl_user_name, sl_api_key, datacenter = sl_data_center)

	dump_container = 'weather_dump'
	archive_container = 'weather_archive'

	#grab all fetchtimes
	w_files_pre = sl_storage[dump_container].objects()
	w_files = []

	if len(w_files_pre) > 2:
		w_files_pre = w_files_pre[:2]

	print "Grabbing list of files..."
	for pre in w_files_pre:
		prestr = pre.__str__()
		try:
			match = re.search(dump_container + ', (.*)\.txt', prestr)
			w_files.append(match.group(1) + '.txt')
		except:
			pass


	#for each file
	for w_file in w_files:
		print "Reading " + w_file
		#read file from object_store and write to to_parse
		newLocalFile = open('to_parse/'+w_file, "w")
		swiftObject = sl_storage[dump_container][w_file].read()
		newLocalFile.write(swiftObject)
		newLocalFile.close()

		print "Parsing..."
		parse_weather.main(w_file, col_names, values)

	to_send = w_files

	print "Sending files for archive container..."
	for i, item in enumerate(to_send):
		print "Sending file " + str(i + 1) + ": " + item
		fullfilename = 'to_parse/' + item
		with open(fullfilename, 'r') as readfile:
			sl_storage[archive_container][readfile.name.split('/')[-1]].send(readfile)
	print 'Data files sent to object store.'


	#if they got there safely... 
	#delete them from local disk and remove them from dump_container
	sentfiles_pre = sl_storage[archive_container].objects()
	sentfiles = []
	for pre in sentfiles_pre:
		prestr = pre.__str__()
		try:
			match = re.search(archive_container + ', (.*)\.txt', prestr)
			sentfiles.append(match.group(1) + '.txt')
		except:
			pass
	print sentfiles

	success = True
	for t in to_send:
		if t not in sentfiles:
			success = False

	if success:
		#delete locally
		for f in to_send:
			os.remove('to_parse/'+f)
		print 'Deleted local files corresponding to ' + date + '.'
		
		#delete from dump_container
		for f in to_send:
			sl_storage[dump_container][f].delete()
		print 'Deleted dump files corresponding to ' + date + '.'


	else:
		print 'Error: all files corresponding to ' + date + ' did not get to ' + archive_container + '. Try again later.'



forecast_cols = ["clouds", "detailed_status", "dewpoint", "heat_index", "humidex", "humidity", "pressure_pres", "pressure_sea_level", "rain", "reference_time", "snow", "status", "sunrise_time", "sunset_time", "temperature_temp", "temperature_temp_kf", "temperature_temp_max", "temperature_temp_min", "visibility_dist", "weather_code", "wind_deg", "wind_speed"]

col_names = ['reception_time', 'location']
for i in range(40):
	col_names += [c+"_"+str((i+1)*3) for c in forecast_cols]
col_names = "("+", ".join(col_names)+")"

values = ["%s" for i in range(len(col_names))]
values = "("+", ".join(values)+")"

#weather_pipeline(col_names, values)



if __name__ == '__main__':
	pass