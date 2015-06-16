#call like this: python bulk_send.py bart
#or: python bulk_send.py weather

from swift_cred import sl_storage
import os, sys
import re

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


if __name__ == '__main__':
	if sys.argv[1] == 'bart':
		container = 'bart_dump'
		bulk_send(maindir = 'data', to_send = to_send('data', container), container = container)
	elif sys.argv[1] == 'weather':
		container = 'weather_dump'
		bulk_send(maindir = 'data', to_send = to_send('data', container), container = container)
	else:
		print "You need to specify an option: 'bart' or 'weather'."