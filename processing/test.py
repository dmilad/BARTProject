import object_storage
import re
import os
import time, datetime
from swift_cred import sl_user_name, sl_api_key, sl_data_center


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

#for f in fts:
#	sl_storage[dump_container][f].delete()




#to_send = os.walk('../data/sentdata').next()[1]
#to_send = [x for x in to_send if '2015-06-29' in x]
#print to_send

#print "Sending files for archive container..."
#for i, item in enumerate(to_send):
#	print "Sending file " + str(i + 1) + ": " + item
#	fullfilename = 'to_parse/' + item
#	with open(fullfilename, 'r') as readfile:
#		sl_storage[archive_container][readfile.name.split('/')[-1]].send(readfile)
#print 'Data files sent to object store.'