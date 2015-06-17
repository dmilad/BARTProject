import time, datetime
from swift_cred import sl_storage
import os, sys
import re

def new_send(maindir = 'data'):
	here = [f for f in os.walk(maindir).next()[2] if f.endswith(".txt")]
	#print here

	today = (datetime.datetime.today()).date()
	fetchtoday = [f for f in os.walk(maindir).next()[2] if f == 'fetch_times_' + str(today) + '.txt']
	#print fetchtoday

	move = list(set(here) - set(fetchtoday))
	#print move

	#send here
	for filetosend in here:
		fullfilename = 'data/' + filetosend
		with open(fullfilename, 'r') as readfile:
			print "Sending " + fullfilename
			sl_storage['send_test'][readfile.name.split('/')[-1]].send(readfile)
	print 'All data files sent to object store.'

	#move move to sentdata
	for filetomove in move:
		os.rename(maindir+'/'+filetomove, maindir+'/sentdata/'+filetomove)
	print "Last hour's data moved."


	


def send_to_storage(api_urls, current = True):

	with open(api_urls, 'r') as urls:

		#record the last fetch hour (so if now is 3pm, last hour was 2pm.)
		fetchtime = datetime.datetime.today() - datetime.timedelta(hours = 1)
		fetchtime = datetime.datetime.strptime('Jun 4 2015  12:00:01AM', '%b %d %Y %I:%M:%S%p') - datetime.timedelta(hours = 1)
		fetchhour = fetchtime.hour
		#record last hour's date
		today = (fetchtime).date()

		#for each listed api command
		for line in urls:

			#grab name of command
			filename, url = line.split('\t')

			#construct last hour's file name using appropriate date and hour of request
			fullfilename = 'data/' + filename + '_' + str(today) + '_' + str(fetchhour) + '.txt'

			#send the file to bart_dump container
			with open(fullfilename, 'r') as readfile:
				print "Sending " + fullfilename
				sl_storage['send_test'][readfile.name.split('/')[-1]].send(readfile)

		print 'All data files sent to object store.'

		#contruct fetchtime file name associate with the last hour
		fullfilename = 'data/fetch_times_' + str(today) + '.txt'
		
		#send the fetchtime file name, even if we are in still in the same day
		with open(fullfilename, 'r') as readfile:
			print "Sending " + fullfilename
			sl_storage['bart_dump'][readfile.name.split('/')[-1]].send(readfile) 
		print 'Fetch times file sent to object store.'


	"""
	sends 
	"""
	"""
	with open(latlongs, 'r') as locs:

		#record the last fetch hour (so if now is 3pm, last hour was 2pm.)
		fetchtime = datetime.datetime.strptime('Jun 16 2015  1:33:01PM', '%b %d %Y %I:%M:%S%p')

		#fetchtime = datetime.datetime.today()
		fetchhour = fetchtime.hour
		today = (fetchtime).date()
		print fetchtime
		print fetchhour
		print today

		fetchtime = datetime.datetime.strptime('Jun 16 2015  1:33:01PM', '%b %d %Y %I:%M:%S%p') - datetime.timedelta(hours = 1)

		#fetchtime = datetime.datetime.today() - datetime.timedelta(hours = 1)
		fetchhour = fetchtime.hour
		today = (fetchtime).date()
		print fetchtime
		print fetchhour
		print today

		#for each listed location
		for line in locs:

			#grab name of command
			loc, lat, lon = line.split(': ')[0], float(line.split(': ')[1].split(', ')[0]), float(line.split(': ')[1].split(', ')[1])

			#construct last hour's file name using appropriate date and hour of request
			if current:
				fullfilename = 'data/' + loc + '_current_'+ str(today) + '_' + str(fetchhour) +'.txt'
			else:
				fullfilename = 'data/' + loc + '_forecast_'+ str(today) + '_' + str(fetchhour) +'.txt'

			print fullfilename
			#send the file to bart_dump container
			#with open(fullfilename, 'r') as readfile:
			#	print "Sending " + fullfilename
				#sl_storage['send_test'][readfile.name.split('/')[-1]].send(readfile)

		print 'All data files sent to object store.'
	"""

#call like this: python bulk_send.py bart
#or: python bulk_send.py weather


def to_send(maindir, container):
	
	"""
	figure out which files to send, and return a list containing the names of those files.
	"""
	
	#grab list of files locally
	here = [f for f in os.walk(maindir).next()[2] if f.endswith(".txt")]
	#grab list of all fetch files locally
	fetchhere = [f for f in os.walk(maindir).next()[2] if f.startswith("fetch")]


	there = []
	#grab list of remote files as objects
	there_pre = sl_storage[container].objects()

	#convert objects to string, add to "there"
	for pre in there_pre:
		prestr = pre.__str__()
		match = re.search(container + ', (.*)\.txt', prestr)
		there.append(match.group(1) + '.txt')

	#create list of files to send: all files here that are not there, but including all fetch files
	to_send = list(set(list(set(here) - set(there)) + fetchhere))
	print "Number of files to send:", str(len(to_send))
	return to_send


def bulk_send(maindir, to_send, container):
	
	"""
	send files one by one to remote object store.
	"""

	for i, item in enumerate(to_send):
		print "Sending file " + str(i + 1) + ": " + item
		fullfilename = maindir + '/' + item

		with open(fullfilename, 'r') as readfile:
			sl_storage[container][readfile.name.split('/')[-1]].send(readfile)
	print 'Data files sent to object store.'


#container = 'send_test'
#fs = to_send('data', container)
#for f in fs:
#	print 'data' + '/' + f
#bulk_send(maindir = 'data', to_send = to_send('data', container), container = container)

#send_to_storage('api_urls.txt')
new_send()

