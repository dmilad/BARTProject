import object_storage
import re
import os
import time, datetime
from swift_cred import sl_user_name, sl_api_key, sl_data_center
import parse_bart
import parse_weather


def bart_pipeline():
	#create storage client object
	sl_storage = object_storage.get_client(sl_user_name, sl_api_key, datacenter = sl_data_center)

	dump_container = 'bart_dump'
	archive_container = 'bart_archive'

	#grab all fetchtimes
	fts_pre = sl_storage[dump_container].objects()
	fts = []

	print "Grabbing list of fetch_time files..."
	for pre in fts_pre:
		prestr = pre.__str__()
		try:
			match = re.search(dump_container + ', (fetch_times_.*)\.txt', prestr)
			fts.append(match.group(1) + '.txt')
		except:
			pass

	#for each fetchtime
	for ft in fts:
		print "Reading " + ft
		#read fetchtime file from object_store and write to to_parse
		newLocalFile = open('to_parse/'+ft, "w")
		swiftObject = sl_storage[dump_container][ft].read()
		newLocalFile.write(swiftObject)
		newLocalFile.close()

		#grab corresponding data files
		date = ft[12:22]


		datafiles_pre = sl_storage[dump_container].objects()
		datafiles = []
		print "Grabbing data files corresponding to " + date + "..."
		for pre in datafiles_pre:
			prestr = pre.__str__()
			try:
				match = re.search(dump_container + ', (.*_' + date + '_.*)\.txt', prestr)
				datafiles.append(match.group(1) + '.txt')
			except:
				pass

		for datafile in datafiles:
			newLocalFile = open('to_parse/'+datafile, "w")
			swiftObject = sl_storage[dump_container][datafile].read()
			newLocalFile.write(swiftObject)
			newLocalFile.close()


		#parse them and write to tables
		print "Parsing..."
		parse_bart.main(date)


		#move all to archive_container
		to_send = datafiles
		to_send.append(ft)

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



def weather_pipeline(col_names, values):
	#create storage client object
	sl_stor = object_storage.get_client(sl_user_name, sl_api_key, datacenter = sl_data_center)

	dump_container = 'weather_dump'
	archive_container = 'weather_archive'

	#grab list of file names
	w_files_full = sl_stor[dump_container].objects()

	#process 100 max at a time
	while len(w_files_full) > 10:

		w_files_pre = w_files_full[:100]
		sl_storage = object_storage.get_client(sl_user_name, sl_api_key, datacenter = sl_data_center)

		w_files = []
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

			sl_storage2 = object_storage.get_client(sl_user_name, sl_api_key, datacenter = sl_data_center)

			print "Reading " + w_file
			#read file from object_store and write to to_parse_weather
			newLocalFile = open('to_parse_weather/'+w_file, "w")
			swiftObject = sl_storage2[dump_container][w_file].read()
			newLocalFile.write(swiftObject)
			newLocalFile.close()

			print "Parsing..."
			parse_weather.main(w_file, col_names, values)

			sl_storage2[dump_container][w_file].delete()
			print 'Deleted '+ w_file +' from dump container.'

			print "Sending file "+ w_file + " to archive container."
			with open('to_parse_weather/' + w_file, 'r') as readfile:
				sl_storage2[archive_container][w_file].send(readfile)

			os.remove('to_parse_weather/'+w_file)
			print 'Deleted '+ w_file +' from local machine.'


		w_files_full = sl_storage2[dump_container].objects()



def start(col_names, values):
	#the purpose of the "started" boolean is to make sure we dont send same files twice
	bart_started = False
	weather_started = False

	while True:
		#used to time 30 seconds between requests
		start_time = time.time()
		fetchtime = datetime.datetime.today()

 		#in the fifteenth minute of every hour, start the bart pipeline. 
		try:
			if fetchtime.minute == 15 and not bart_started:
				print "Start bart pipeline..."
				bart_pipeline()
				bart_started = True
			else:
				bart_started = False
		except Exception, e:
			print "An error occurred. BART parsing pipeline could not initiate at " + str(fetchtime), e

 		#in the twentieth minute of every hour, start the weather pipeline. 
		try:
			if fetchtime.minute == 20 and not weather_started:
				print "Start weather pipeline..."
				weather_pipeline(col_names, values)
				weather_started = True
			else:
				weather_started = False
		except Exception, e:
			print "An error occurred. Weather parsing pipeline could not initiate at " + str(fetchtime), e

		#time 30 seconds between requests
		end_time = time.time()
		duration = end_time - start_time 
		time.sleep(max([0, 30 - duration]))


if __name__ == "__main__":

	#some variables defined for weather
	forecast_cols = ["clouds", "detailed_status", "dewpoint", "heat_index", "humidex", "humidity", "pressure_pres", "pressure_sea_level", "rain", "reference_time", "snow", "status", "sunrise_time", "sunset_time", "temperature_temp", "temperature_temp_kf", "temperature_temp_max", "temperature_temp_min", "visibility_dist", "weather_code", "wind_deg", "wind_speed"]

	col_names = ['reception_time', 'location']
	for i in range(40):
		col_names += [c+"_"+str((i+1)*3) for c in forecast_cols]

	values = ["%s" for i in range(len(col_names))]

	col_names = "("+", ".join(col_names)+")"
	values = "("+", ".join(values)+")"

	weather_pipeline(col_names, values)
	#run this forever
	while True:

		#if there is an error, try again in 60 seconds
		try:
			print "Starting..."
			start(col_names, values)
		except Exception, e:
			print "Error: ", e
			print "Retrying in 60 seconds..."
		time.sleep(60)
