import xmltodict
import datetime
import pandas as pd
#import MySQLdb
from mysql_cred import mysql_user, mysql_passowrd
#from pprint import pprint


"""
Currently all BSA messages have a station code of "BART" signifying that they are system-wide. If future messages are tagged for specific stations, and a station is specified in the orig parameter, information will be provided for the specified station as well as any system-wide tagged messages. To get all messages, regardless of tags, specify "orig=all".

The "type" element will either be DELAY or EMERGENCY. The "sms_text" element is a contracted version of the "description" element for use with text messaging. This element might be longer than allowed in a single text message, so it might need to be broken up into several messages.

Sample

With delay message
<?xml version="1.0" encoding="iso-8859-1"?>
<root>
  <uri>
	<![CDATA[http://api.bart.gov/api/bsa.aspx?cmd=bsa&date=today]]>
  </uri>
  <date>08/29/2013</date>
  <time>11:55:00 AM PDT</time>
  <bsa id="112978">
	<station>BART</station>
	<type>DELAY</type>
	<description>
	  <![CDATA[BART is running round-the-clock service during the labor day weekend bay bridge closure. More info at www.bart.gov or (510) 465-2278.]]>
	</description>
	<sms_text>
	  <![CDATA[BART is running round-the-clock svc during labor day weekend bay bridge closure. More info at www.BART.gov or (510) 465-2278.]]>
	</sms_text>
	<posted>Wed Aug 28 2013 10:44 PM PDT</posted>
	<expires>Thu Dec 31 2037 11:59 PM PST</expires>
  </bsa>
  <message></message>
</root>
"""

#con = MySQLdb.connect("localhost", mysql_user, mysql_passowrd, "test")

fetchtimes = []
with open('../data_collection/data/sentdata/fetch_times_2015-06-03.txt', 'r') as readfile:
	for line in readfile:
		fetchtimes.append(line.rstrip())

date_hours = list(set([f[:13] for f in fetchtimes]))
print fetchtimes
#2015-06-03 23
print date_hours

for date_hour in date_hours:
	fetchtimes_subset = [f for f in fetchtimes if f[:13] == date_hour]

	with open('../data_collection/data/sentdata/adv_bsa_' + date_hour.replace(' ', '_') + '.txt', 'r') as readfile:
		content = readfile.read().split('\n\n')

		to_db = []

		for i, c in enumerate(content[:-1]):

			print i
			fetchtime = fetchtimes_subset[i]
			raw = xmltodict.parse(c)

			date = raw['root']['date']
			time = raw['root']['time']

			bsa_type = type(raw['root']['bsa']) #list or collections.OrderedDict
			#print bsa_type
			if bsa_type == list:
				for bsa_content in raw['root']['bsa']:
					_id = bsa_content['@id']
					station = bsa_content['station']
					_type = bsa_content['type']
					description = bsa_content['description']
					posted = bsa_content['posted']
					expires = bsa_content['expires']
					to_db.append((fetchtime, date, time, _id, station, _type, description, posted, expires))

			else:
				try:
					bsa_content = raw['root']['bsa']
					_id = bsa_content['@id']
					station = bsa_content['station']
					_type = bsa_content['type']
					description = bsa_content['description']
					posted = bsa_content['posted']
					expires = bsa_content['expires']
					to_db.append((fetchtime, date, time, _id, station, _type, description, posted, expires))

				except:
					bsa_content = raw['root']['bsa']
					_id = 'Null'
					station = 'Null'
					_type = 'Null'
					description = bsa_content['description']
					posted = 'Null'
					expires = 'Null'
					to_db.append((fetchtime, date, time, _id, station, _type, description, posted, expires))


	print len(to_db)
	print to_db
					#cursor = con.cursor()
					#cursor.executemany("""INSERT INTO adv_bsa (date, time, id, station, type, description, posted, expires) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""", to_db) # note the two arguments
					#cursor.close()
	#con.commit()
	#con.close()


					#df = pd.DataFrame(columns = ['date', 'time', 'id', 'station', 'type', 'description', 'posted', 'expires'])
					#df_append = pd.DataFrame({'date': [date], 'time': [time], 'id': [_id], 'station': [station], 'type': [_type], 'description': [description], 'posted': [posted], 'expires': [expires]})
					#df = df.append(df_append, ignore_index=True)
					#print df.head
					#df.to_sql(con = con, name = 'adv_bsa', if_exists = 'append', flavor = 'mysql')
				




