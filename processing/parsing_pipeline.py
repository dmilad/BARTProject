import object_storage
import re
import os
import time, datetime
from swift_cred import sl_user_name, sl_api_key, sl_data_center
import parse_bart


def bart_pipeline():
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



def start():
	started = False
	while True:
		#used to time 30 seconds between requests
		start_time = time.time()
		fetchtime = datetime.datetime.today()

 		#in the fifteenth minute of every hour, start the pipeline. 
		#the purpose of the "started" boolean to make sure we dont send same files twice in the first minute
		try:
			if fetchtime.minute == 15 and not started:
				bart_pipeline()
				started = True
			else:
				started = False
		except Exception, e:
			print "An error occurred. Parsing pipeline could not initiate at " + str(fetchtime), e

		#time 30 seconds between requests
		end_time = time.time()
		duration = end_time - start_time 
		time.sleep(max([0, 30 - duration]))


if __name__ == "__main__":

	#run this forever
	while True:

		#if there is an error, try again in 60 seconds
		try:
			start()
		except Exception, e:
			print "Error: ", e
			print "Retrying in 60 seconds..."
		time.sleep(60)