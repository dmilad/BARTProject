import object_storage
import re
import os
import time, datetime
from swift_cred import sl_user_name, sl_api_key, sl_data_center


def trans():
	sl_storage = object_storage.get_client(sl_user_name, sl_api_key, datacenter = sl_data_center)

	dump_container = 'weather_dump'
	archive_container = 'weather_archive'

	#grab all files
	fts_pre = sl_storage[archive_container].objects()
	fts = []

	print "Grabbing list of fetch_time files..."
	for pre in fts_pre:
		prestr = pre.__str__()
		try:
			match = re.search(archive_container + ', (.*)\.txt', prestr)
			fts.append(match.group(1) + '.txt')
		except:
			pass

	print fts

	#for each fetchtime
	for ft in fts:
		print "Reading " + ft
		#read fetchtime file from object_store and write to to_parse
		newLocalFile = open('to_parse/'+ft, "w")
		swiftObject = sl_storage[archive_container][ft].read()
		newLocalFile.write(swiftObject)
		newLocalFile.close()

	to_send = fts

	print "Sending files for dump container..."
	for i, item in enumerate(to_send):
		print "Sending file " + str(i + 1) + ": " + item
		fullfilename = 'to_parse/' + item
		with open(fullfilename, 'r') as readfile:
			sl_storage[dump_container][readfile.name.split('/')[-1]].send(readfile)
	print 'Data files sent to object store.'

trans()