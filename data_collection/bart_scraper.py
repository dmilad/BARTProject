import os
import requests
import time, datetime
import object_storage
from swift_cred import sl_user_name, sl_api_key, sl_data_center 

def fetch(api_urls):

	"""
	generate BART API commands, fetch the response for each, and write to appropriate text files. 
	"""

	api_key='MW9S-E7SL-26DU-VV8V'

	#fetch response for all api commands listed in the api_urls file
	with open(api_urls, 'r') as urls:
		
		#record fetch time (to be returned by the function and written to the appropriate fetchtime file)
		fetchtime = datetime.datetime.today()
		#record hour of the fetch (to use in file name)
		fetchhour = fetchtime.hour
		#record today's daye (to use in file name)
		today = fetchtime.date()

		#for each listed api command
		for line in urls:

			#grab name of command, and the url
			filename, url = line.split('\t')
			url = url.rstrip()

			#for each command, construct the appropriate file name containing the name of command, date, and hour of request
			with open('data/' + filename + '_' + str(today) + '_' + str(fetchhour) + '.txt', 'a') as writefile:
				response = requests.get(url + '&key=' + api_key)
				writefile.write(response.text + '\n\n')
			print str(fetchtime), filename, url + '&key=' + api_key
	return fetchtime


def send_to_storage(maindir = 'data'):

	"""
	sends 
	"""

	sl_storage = object_storage.get_client(sl_user_name, sl_api_key, datacenter = sl_data_center)

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
			sl_storage['bart_dump'][readfile.name.split('/')[-1]].send(readfile)
	print 'All data files sent to object store.'

	#move move to sentdata
	for filetomove in move:
		os.rename(maindir+'/'+filetomove, maindir+'/sentdata/'+filetomove)
	print "Last hour's data moved."


def start():
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

		#call fetch, which makes api calls based on the urls provided in the api_urls files
		#record the fetch time in the appropriate file, constructed using the date of fetch (one fetchfile per date)
		try:
			fetchtime = fetch('api_urls.txt')
			with open('data/fetch_times_' + str(fetchtime.date()) + '.txt', 'a') as fetchtimefile:
				fetchtimefile.write(str(fetchtime) + '\n')
		except:
			pass

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
