import os
import requests
import time, datetime
import object_storage
from swift_cred import sl_storage

def fetch():
	api_key='MW9S-E7SL-26DU-VV8V'
	with open('api_urls.txt', 'r') as urls:
		fetchtime = datetime.datetime.today()
		fetchhour = fetchtime.hour
		today = datetime.date.today()
		for line in urls:
			filename, url = line.split('\t')
			url = url.rstrip()
			with open('data/'+filename+'_'+str(today)+'_'+str(fetchhour)+'.txt', 'a') as writefile:
				response = requests.get(url+'&key='+api_key)
				writefile.write(response.text+'\n\n')
			print filename
			print url+'&key='+api_key
	return fetchtime

def send_to_storage():
	with open('api_urls.txt', 'r') as urls:
		fetchtime = datetime.datetime.today() - datetime.timedelta(hours = 1)
		fetchhour = fetchtime.hour
		today = (datetime.datetime.today() - datetime.timedelta(hours = 1)).date()
		for line in urls:
			filename, url = line.split('\t')

			fullfilename = 'data/'+filename+'_'+str(today)+'_'+str(fetchhour)+'.txt'
			with open(fullfilename, 'r') as readfile:
				sl_storage['bart_dump'][readfile.name.split('/')[-1]].send(readfile)
		print 'Data files sent to object store.'

		fullfilename = 'data/fetch_times_'+str(today)+'.txt'
		with open(fullfilename, 'r') as readfile:
			sl_storage['bart_dump'][readfile.name.split('/')[-1]].send(readfile) 
		print 'Fetch times file sent to object store.'

def start():
	sent = False
	while True:
		start_time = time.time()
		try:
			fetchtime = fetch()
			with open('data/fetch_times_'+str(fetchtime.date())+'.txt', 'a') as fetchtimefile:
				fetchtimefile.write(str(fetchtime)+'\n')
		except:
			pass
		try:
			if fetchtime.minute == 0 and not sent:
				send_to_storage()
				sent = True
			else:
				sent = False
		except Exception, e:
			print "An error occurred. Files could not be sent to object store.", e

		end_time = time.time()
		duration = end_time - start_time 
		time.sleep(max([0, 30 - duration]))

if __name__ == '__main__':
	while True:
		try:
			start()
		except Exception, e:
			print "error: ", e
			print "retrying in 60 seconds..."
		time.sleep(60)
