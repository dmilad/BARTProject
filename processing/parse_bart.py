import xmltodict
import datetime
import MySQLdb
from mysql_cred import mysql_user, mysql_passowrd, mysql_database

def adv_bsa(date_hour, fetchtimes_subset, con):
	"""
	Currently all BSA messages have a station code of "BART" signifying that they are system-wide. If future messages are tagged for specific stations, and a station is specified in the orig parameter, information will be provided for the specified station as well as any system-wide tagged messages. To get all messages, regardless of tags, specify "orig=all".

	The "type" element will either be DELAY or EMERGENCY. The "sms_text" element is a contracted version of the "description" element for use with text messaging. This element might be longer than allowed in a single text message, so it might need to be broken up into several messages.

	Sample with delay message
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

	with open('to_parse/adv_bsa_' + date_hour.split(' ')[0] + '_' + str(int(date_hour.split(' ')[1])) + '.txt', 'r') as readfile:
		content = readfile.read().split('\n\n')

		to_db = []

		for i, c in enumerate(content[:-1]):

			fetchtime = fetchtimes_subset[i]
			raw = xmltodict.parse(c)

			try:
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
			except:
				print "There was a parsing error with a record, Skipping."

		cursor = con.cursor()
		cursor.executemany("""INSERT INTO adv_bsa (fetchtime, date, time, id, station, type, description, posted, expires) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""", to_db)
		cursor.close()
		print "\t\tadv_bsa - Records written: " + str(len(to_db))


def adv_count(date_hour, fetchtimes_subset, con):
	"""
	XML results Sample

	<?xml version="1.0" encoding="utf-8" ?> 
	<root>
	  <uri><![CDATA[ http://api.bart.gov/api/bsa.aspx?cmd=count ]]></uri> 
	  <date>10/14/2009</date> 
	  <time>15:52:00 PM PDT</time> 
	  <traincount>51</traincount> 
	  <message /> 
	</root>
	"""
	
	with open('to_parse/adv_count_' + date_hour.split(' ')[0] + '_' + str(int(date_hour.split(' ')[1])) + '.txt', 'r') as readfile:
		content = readfile.read().split('\n\n')

		to_db = []

		for i, c in enumerate(content[:-1]):

			fetchtime = fetchtimes_subset[i]
			raw = xmltodict.parse(c)

			try:
				date = raw['root']['date']
				time = raw['root']['time']
				traincount = raw['root']['traincount']
				to_db.append((fetchtime, date, time, traincount))
			except:
				print "There was a parsing error with a record, Skipping."

		cursor = con.cursor()
		cursor.executemany("""INSERT INTO adv_count (fetchtime, date, time, traincount) VALUES (%s, %s, %s, %s)""", to_db)
		cursor.close()
		print "\t\tadv_count - Records written: " + str(len(to_db))


def adv_elev(date_hour, fetchtimes_subset, con):
	"""
	XML results sample

	<?xml version="1.0" encoding="iso-8859-1" ?> 
	<root>
	  <uri><![CDATA[ http://api.bart.gov/api/bsa.aspx?cmd=elev ]]></uri> 
	  <date>10/14/2009</date> 
	  <time>15:57:00 PM PDT</time> 
	  <bsa>
	    <description><![CDATA[ Attention passengers: All elevators are in service. Thank You. ]]></description>
	    <sms_text><![CDATA[ ALL ELEVS ARE IN SVC. ]]></sms_text>
	  </bsa>
	  <message /> 
	</root>
	"""
	
	with open('to_parse/adv_elev_' + date_hour.split(' ')[0] + '_' + str(int(date_hour.split(' ')[1])) + '.txt', 'r') as readfile:
		content = readfile.read().split('\n\n')

		to_db = []

		for i, c in enumerate(content[:-1]):

			fetchtime = fetchtimes_subset[i]
			raw = xmltodict.parse(c)

			try:
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
			except:
				print "There was a parsing error with a record, Skipping."

		cursor = con.cursor()
		cursor.executemany("""INSERT INTO adv_elev (fetchtime, date, time, id, station, type, description, posted, expires) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""", to_db)
		cursor.close()
		print "\t\tadv_elev - Records written: " + str(len(to_db))


def sched_special(date_hour, fetchtimes_subset, con):
	"""
	Occasionally BART has special schedule announcements that affect certain stations (construction, etc.). If a trip is requested from the API, a message will be appended to the trip results stating that a special message may pertain to the trip. This API call provides all current and future special schedule notices. A notice is considered current or future if the end date is greater than or equal to the date the call is made.

	XML results sample

	<?xml version="1.0" encoding="utf-8" ?> 
	<root>
	  <uri><![CDATA[ http://api.bart.gov/api/sched.aspx?cmd=special&l=1 ]]></uri>
	  <special_schedules>
	    <special_schedule>
	      <start_date>08/09/2009</start_date> 
	      <end_date>09/13/2009</end_date> 
	      <start_time>03:45</start_time> 
	      <end_time>19:15</end_time> 
	      <text>Weekday trains may arrive later than scheduled at the Dublin/Pleasanton station due to construction.</text> 
	      <link><![CDATA[ http://www.bart.gov/news/articles/2007/news20070705.aspx ]]></link>
	      <orig>DUBL</orig>
	      <dest>DUBL</dest> 
	      <day_of_week /> 
	      <routes_affected>ROUTE 1,ROUTE 2</routes_affected>
	    </special_schedule>
	  </special_schedules>
	  <message>
	    <legend>day_of_week: 0 = Sunday, 1 = Monday, 2 = Tuesday, 3 = Wednesday, 4 = Thursday, 5 = Friday, 6 = Saturday, blank = all days</legend> 
	  </message>
	</root>
	Notes

	If there is a related news article, then the link element will provide a URL to a BART news story.
	"""
	
	with open('to_parse/sched_special_' + date_hour.split(' ')[0] + '_' + str(int(date_hour.split(' ')[1])) + '.txt', 'r') as readfile:
		content = readfile.read().split('\n\n')

		to_db = []

		for i, c in enumerate(content[:-1]):

			fetchtime = fetchtimes_subset[i]
			raw = xmltodict.parse(c)

			try:
				schedules = raw['root']['special_schedules']['special_schedule']
				for special_schedule in schedules:
					#special_schedule = schedule['special_schedule']
					start_date = special_schedule['start_date']
					end_date = special_schedule['end_date']
					start_time = special_schedule['start_time']
					end_time = special_schedule['end_time']
					text_ = special_schedule['text']
					link = special_schedule['link']
					orig = special_schedule['orig']
					dest = special_schedule['dest']
					day_of_week = special_schedule['day_of_week']
					routes_affected = special_schedule['routes_affected']
					to_db.append((fetchtime, start_date, end_date, start_time, end_time, text_, link, orig, dest, day_of_week, routes_affected))
			
			except:
				print "There was a parsing error with a record, Skipping."


		cursor = con.cursor()
		cursor.executemany("""INSERT INTO sched_special (fetchtime, start_date, end_date, start_time, end_time, text_, link, orig, dest, day_of_week, routes_affected) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", to_db)
		cursor.close()
		print "\t\tsched_special - Records written: " + str(len(to_db))


def rout_routes(date_hour, fetchtimes_subset, con):
	"""
	Route Information sometimes changes with schedule changes as routes are reconfigured. This may affect the name and abbreviation of the route.
	The optional "date" and "sched" parameters should not be used together. If they are, the date will be ignored, and the sched parameter will be used.
	
	XML results sample

	<?xml version="1.0" encoding="utf-8" ?> 
	<root>
	  <uri><![CDATA[ http://api.bart.gov/api/route.aspx?cmd=routes ]]></uri>
	  <sched_num>26</sched_num> 
	  <routes>
	    <route>
	      <name>Pittsburg/Bay Point - SFIA/Millbrae</name> 
	      <abbr>PITT-SFIA</abbr> 
	      <routeID>ROUTE 1</routeID> 
	      <number>1</number> 
	      <color>#ffff33</color> 
	    </route>
	    ...
	  </routes>
	  <message /> 
	</root>
	"""
	
	with open('to_parse/rout_routes_' + date_hour.split(' ')[0] + '_' + str(int(date_hour.split(' ')[1])) + '.txt', 'r') as readfile:
		content = readfile.read().split('\n\n')

		to_db = []

		for i, c in enumerate(content[:-1]):

			fetchtime = fetchtimes_subset[i]
			raw = xmltodict.parse(c)

			try:
				sched_num = raw['root']['sched_num']

				routes = raw['root']['routes']['route']
				for route in routes:
					#route = r['route']
					name = route['name']
					abbr = route['abbr']
					routeid = route['routeID']
					number = route['number']
					color = route['color']
					to_db.append((fetchtime, sched_num, name, abbr, routeid, number, color))

			except:
				print "There was a parsing error with a record, Skipping."


		cursor = con.cursor()
		cursor.executemany("""INSERT INTO rout_routes (fetchtime, sched_num, name, abbr, routeid, number, color) VALUES (%s, %s, %s, %s, %s, %s, %s)""", to_db)
		cursor.close()
		print "\t\trout_routes - Records written: " + str(len(to_db))


def rout_routeinfo(date_hour, fetchtimes_subset, con):
	"""
	Route information is sometimes updated with schedule changes as routes are reconfigured. This may affect the name and abbreviation of the route, as well as the number of stations.
	The optional "date" and "sched" parameters should not be used together. If they are, the date will be ignored, and the sched parameter will be used.
	XML results sample

	<?xml version="1.0" encoding="utf-8" ?>
	<root>
	  <uri> http://api.bart.gov/api/route.aspx?cmd=routeinfo&route=6 </uri>
	  <sched_num>26</sched_num>
	  <routes>
	    <route>
	      <name>Daly City - Fremont</name>
	      <abbr>DALY-FRMT</abbr>
	      <routeID>ROUTE 6</routeID>
	      <number>6</number>
	      <origin>DALY</origin>
	      <destination>FRMT</destination>
	      <direction>south</direction>
	      <color>#339933</color>
	      <holidays>0</holidays>
	      <num_stns>19</num_stns>
	      <config>
	        <station>DALY</station>
	        <station>BALB</station>
	        <station>GLEN</station>
	        <station>24TH</station>
	        <station>16TH</station>
	        <station>CIVC</station>
	        <station>POWL</station>
	        <station>MONT</station>
	        <station>EMBR</station>
	        <station>WOAK</station>
	        <station>LAKE</station>
	        <station>FTVL</station>
	        <station>COLS</station>
	        <station>SANL</station>
	        <station>BAYF</station>
	        <station>HAYW</station>
	        <station>SHAY</station>
	        <station>UCTY</station>
	        <station>FRMT</station>
	      </config>
	    </route>
	  </routes>
	  <message />
	</root>
	"""
	
	with open('to_parse/rout_routeinfo_' + date_hour.split(' ')[0] + '_' + str(int(date_hour.split(' ')[1])) + '.txt', 'r') as readfile:
		content = readfile.read().split('\n\n')

		to_db = []

		for i, c in enumerate(content[:-1]):

			fetchtime = fetchtimes_subset[i]
			raw = xmltodict.parse(c)

			try:
				sched_num = raw['root']['sched_num']

				routes = raw['root']['routes']['route']
				for route in routes:
					#route = r['route']
					name = route['name']
					abbr = route['abbr']
					routeid = route['routeID']
					number = route['number']
					origin = route['origin']
					destination = route['destination']
					direction = route['direction']
					color = route['color']
					holidays = route['holidays']
					num_stns = route['num_stns']

					s = []
					config = route['config']['station']
					for c in config:
						s.append(c)
					stations = ",".join(s)

					to_db.append((fetchtime, sched_num, name, abbr, routeid, number, origin, destination, direction, color, holidays, num_stns, stations))

			except:
				print "There was a parsing error with a record, Skipping."

		cursor = con.cursor()
		cursor.executemany("""INSERT INTO rout_routeinfo (fetchtime, sched_num, name, abbr, routeid, number, origin, destination, direction, color, holidays, num_stns, stations) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", to_db)
		cursor.close()
		print "\t\trout_routeinfo - Records written: " + str(len(to_db))


def rt_etd(date_hour, fetchtimes_subset, con):
	"""
	The optional parameters 'plat' and 'dir' should not be used together. If they are, then the 'dir' parameter will be ignored and just the platform parameter will be used.

	If 'ALL' is used for the orig station, then 'plat' and 'dir' cannot be used.

	XML results sample

	<?xml version="1.0" encoding="utf-8" ?> 
	<root>
	  <uri><![CDATA[ http://api.bart.gov/api/etd.aspx?cmd=etd&orig=RICH ]]></uri>
	  <date>03/30/2011</date> 
	  <time>02:43:27 PM PDT</time> 
	  <station>
	    <name>Richmond</name> 
	    <abbr>RICH</abbr> 
	    <etd>
	      <destination>Fremont</destination> 
	      <abbreviation>FRMT</abbreviation> 
	      <estimate>
	        <minutes>5</minutes> 
	        <platform>2</platform> 
	        <direction>South</direction> 
	        <length>6</length> 
	        <color>ORANGE</color> 
	        <hexcolor>#ff9933</hexcolor> 
	        <bikeflag>1</bikeflag> 
	      </estimate>
	      <estimate>
	        <minutes>20</minutes> 
	        <platform>2</platform> 
	        <direction>South</direction> 
	        <length>6</length> 
	        <color>ORANGE</color> 
	        <hexcolor>#ff9933</hexcolor> 
	        <bikeflag>1</bikeflag> 
	      </estimate>
	    </etd>
	    <etd>
	      <destination>Millbrae</destination> 
	      <abbreviation>MLBR</abbreviation> 
	      <estimate>
	        <minutes>Leaving</minutes> 
	        <platform>2</platform> 
	        <direction>South</direction> 
	        <length>10</length> 
	        <color>RED</color> 
	        <hexcolor>#ff0000</hexcolor> 
	        <bikeflag>1</bikeflag> 
	      </estimate>
	    </etd>
	  </station>
	  <message /> 
	</root>
	Notes

	The <color>,<hexcolor>, and <bikeflag> fields will be added on 4/12/2011. See the Change Log for more information.

	If the combination of 'platform' or 'dir' results in no ETD data, a warning message will be displayed.
	"""
	
	with open('to_parse/rt_etd_' + date_hour.split(' ')[0] + '_' + str(int(date_hour.split(' ')[1])) + '.txt', 'r') as readfile:
		content = readfile.read().split('\n\n')

		to_db = []

		for i, c in enumerate(content[:-1]):

			fetchtime = fetchtimes_subset[i]
			raw = xmltodict.parse(c)

			try:
				date = raw['root']['date']
				time = raw['root']['time']

				station_type = type(raw['root']['station'])

				if station_type == list:
					for station in raw['root']['station']:
						name = station['name']
						abbr = station['abbr']

						etd_type = type(station['etd'])

						if etd_type == list:
							for etd in station['etd']:
								destination = etd['destination']
								abbreviation = etd['abbreviation']

								estimate_type = type(etd['estimate'])

								if estimate_type == list:
									for estimate in etd['estimate']:
										minutes = estimate['minutes']
										platform = estimate['platform']
										direction = estimate['direction']
										length = estimate['length']
										color = estimate['color']
										hexcolor = estimate['hexcolor']
										bikeflag = estimate['bikeflag']
										to_db.append((fetchtime, date, time, name, abbr, destination, abbreviation, minutes, platform, direction, length, color, hexcolor, bikeflag))
								else:
									estimate = etd['estimate']
									minutes = estimate['minutes']
									platform = estimate['platform']
									direction = estimate['direction']
									length = estimate['length']
									color = estimate['color']
									hexcolor = estimate['hexcolor']
									bikeflag = estimate['bikeflag']
									to_db.append((fetchtime, date, time, name, abbr, destination, abbreviation, minutes, platform, direction, length, color, hexcolor, bikeflag))

						else:
							etd = station['etd']
							destination = etd['destination']
							abbreviation = etd['abbreviation']

							estimate_type = type(etd['estimate'])

							if estimate_type == list:
								for estimate in etd['estimate']:
									minutes = estimate['minutes']
									platform = estimate['platform']
									direction = estimate['direction']
									length = estimate['length']
									color = estimate['color']
									hexcolor = estimate['hexcolor']
									bikeflag = estimate['bikeflag']
									to_db.append((fetchtime, date, time, name, abbr, destination, abbreviation, minutes, platform, direction, length, color, hexcolor, bikeflag))
							else:
								estimate = etd['estimate']
								minutes = estimate['minutes']
								platform = estimate['platform']
								direction = estimate['direction']
								length = estimate['length']
								color = estimate['color']
								hexcolor = estimate['hexcolor']
								bikeflag = estimate['bikeflag']
								to_db.append((fetchtime, date, time, name, abbr, destination, abbreviation, minutes, platform, direction, length, color, hexcolor, bikeflag))
				else:
					station = raw['root']['station']
					name = station['name']
					abbr = station['abbr']

					etd_type = type(station['etd'])

					if etd_type == list:
						for etd in station['etd']:
							destination = etd['destination']
							abbreviation = etd['abbreviation']

							estimate_type = type(etd['estimate'])

							if estimate_type == list:
								for estimate in etd['estimate']:
									minutes = estimate['minutes']
									platform = estimate['platform']
									direction = estimate['direction']
									length = estimate['length']
									color = estimate['color']
									hexcolor = estimate['hexcolor']
									bikeflag = estimate['bikeflag']
									to_db.append((fetchtime, date, time, name, abbr, destination, abbreviation, minutes, platform, direction, length, color, hexcolor, bikeflag))
							else:
								estimate = etd['estimate']
								minutes = estimate['minutes']
								platform = estimate['platform']
								direction = estimate['direction']
								length = estimate['length']
								color = estimate['color']
								hexcolor = estimate['hexcolor']
								bikeflag = estimate['bikeflag']
								to_db.append((fetchtime, date, time, name, abbr, destination, abbreviation, minutes, platform, direction, length, color, hexcolor, bikeflag))

					else:
						etd = station['etd']
						destination = etd['destination']
						abbreviation = etd['abbreviation']

						estimate_type = type(etd['estimate'])

						if estimate_type == list:
							for estimate in etd['estimate']:
								minutes = estimate['minutes']
								platform = estimate['platform']
								direction = estimate['direction']
								length = estimate['length']
								color = estimate['color']
								hexcolor = estimate['hexcolor']
								bikeflag = estimate['bikeflag']
								to_db.append((fetchtime, date, time, name, abbr, destination, abbreviation, minutes, platform, direction, length, color, hexcolor, bikeflag))
						else:
							estimate = etd['estimate']
							minutes = estimate['minutes']
							platform = estimate['platform']
							direction = estimate['direction']
							length = estimate['length']
							color = estimate['color']
							hexcolor = estimate['hexcolor']
							bikeflag = estimate['bikeflag']
							to_db.append((fetchtime, date, time, name, abbr, destination, abbreviation, minutes, platform, direction, length, color, hexcolor, bikeflag))

			except:
				print "There was a parsing error with a record, Skipping."

		cursor = con.cursor()
		cursor.executemany("""INSERT INTO rt_etd (fetchtime, date, time, name, abbr, destination, abbreviation, minutes, platform, direction, length, color, hexcolor, bikeflag) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", to_db)
		cursor.close()
		print "\t\trt_etd - Records written: " + str(len(to_db))


def get_record_counts(con):

	cursor = con.cursor()
	cursor.execute("""select count(*) from adv_bsa;""")
	result = cursor.fetchall()
	print "Records in adv_bsa: " + str(result)
	cursor.close()

	cursor = con.cursor()
	cursor.execute("""select count(*) from adv_count;""")
	result = cursor.fetchall()
	print "Records in adv_count: " + str(result)
	cursor.close()

	cursor = con.cursor()
	cursor.execute("""select count(*) from adv_elev;""")
	result = cursor.fetchall()
	print "Records in adv_elev: " + str(result)
	cursor.close()

	cursor = con.cursor()
	cursor.execute("""select count(*) from sched_special;""")
	result = cursor.fetchall()
	print "Records in sched_special: " + str(result)
	cursor.close()

	cursor = con.cursor()
	cursor.execute("""select count(*) from rout_routes;""")
	result = cursor.fetchall()
	print "Records in rout_routes: " + str(result)
	cursor.close()

	cursor = con.cursor()
	cursor.execute("""select count(*) from rout_routeinfo;""")
	result = cursor.fetchall()
	print "Records in rout_routeinfo: " + str(result)
	cursor.close()

	cursor = con.cursor()
	cursor.execute("""select count(*) from rt_etd;""")
	result = cursor.fetchall()
	print "Records in rt_etd: " + str(result)
	cursor.close()


def main(date):
	con = MySQLdb.connect("localhost", mysql_user, mysql_passowrd, mysql_database)

	fetchtimes = []
	with open('to_parse/fetch_times_' + date + '.txt', 'r') as readfile:
		for line in readfile:
			fetchtimes.append(line.rstrip())

	#print fetchtimes
	date_hours = list(set([f[:13] for f in fetchtimes if len(f) > 1]))
	
	cursor = con.cursor()
	cursor.execute("select distinct substring(fetchtime, 1,13) from adv_bsa where fetchtime like '%" + date + "%';")
	result = cursor.fetchall()
	written = [r[0] for r in result]
	cursor.close()
	
	date_hours = list(set(date_hours) - set(written))

	for date_hour in date_hours:
		print "\tParsing files corresponding to " + date_hour + "..."
		fetchtimes_subset = [f for f in fetchtimes if f[:13] == date_hour]

		adv_bsa(date_hour, fetchtimes_subset, con)
		adv_count(date_hour, fetchtimes_subset, con)
		adv_elev(date_hour, fetchtimes_subset, con)
		sched_special(date_hour, fetchtimes_subset, con)
		rout_routes(date_hour, fetchtimes_subset, con)
		rout_routeinfo(date_hour, fetchtimes_subset, con)
		rt_etd(date_hour, fetchtimes_subset, con)

	con.commit()

	get_record_counts(con)

	con.close()


if __name__ == '__main__':
	pass




