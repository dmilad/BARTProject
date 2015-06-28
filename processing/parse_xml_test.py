import xmltodict
import datetime
import pandas as pd
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

with open('../data_collection/data/sentdata/adv_bsa_2015-06-03_23.txt', 'r') as readfile:
	content = readfile.read().split('\n\n')
	count = 0
	for c in content[:-1]:
		count += 1
		if count == 1:
			#print 'c'
			raw = xmltodict.parse(c)
			#print 'r'
			print raw
			#pprint(raw)
			print

			date = raw['root']['date']
			time = raw['root']['time']
			bsa_type = type(raw['root']['bsa']) #list or collections.OrderedDict
			print bsa_type
			if bsa_type == list:
				for bsa_content in raw['root']['bsa']:
					bsa_id = bsa_content['@id']
					#...
			else:
				bsa_content = raw['root']['bsa']
				_id = bsa_content['@id']
				station = bsa_content['station']
				_type = bsa_content['type']
				description = bsa_content['description']
				posted = bsa_content['posted']
				expires = bsa_content['expires']

				df = pd.DataFrame(columns = ['date', 'time', 'id', 'station', 'type', 'description', 'posted', 'expires'])

				df_append = pd.DataFrame({'date': [date], 'time': [time], 'id': [_id], 'station': [station], 'type': [_type], 'description': [description], 'posted': [posted], 'expires': [expires]})

				df = df.append(df_append, ignore_index=True)
				print df.head
			




