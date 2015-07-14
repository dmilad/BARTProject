import simplejson
import datetime
import MySQLdb
from mysql_cred import mysql_user, mysql_passowrd, mysql_database
import re


def get_record_counts(con):

	cursor = con.cursor()
	cursor.execute("""select count(*) from weather_current;""")
	result = cursor.fetchall()
	print "Records in weather_current: " + str(result)
	cursor.close()

	cursor = con.cursor()
	cursor.execute("""select count(*) from weather_forecast;""")
	result = cursor.fetchall()
	print "Records in weather_forecast: " + str(result)
	cursor.close()


def parse_current_weather(w_file, con):
	with open('to_parse_weather/'+w_file, 'r') as jsonfile:
		content = jsonfile.read().split('\n\n')

		to_db = []

		for c in content[:-1]:

			try:
				raw = simplejson.loads(c)

				try:
					reception_time = datetime.datetime.fromtimestamp(int(raw['reception_time'])).strftime('%Y-%m-%d %H:%M:%S')
				except:
					reception_time = 'NULL'
				try:
					location = raw['Location']['name']
				except:
					location = 'NULL'

				try:
					weather = raw['Weather']
				except:
					weather = 'NULL'

				try:
					clouds = str(weather['clouds'])
				except:
					clouds = 'NULL'
				try:
					detailed_status = str(weather['detailed_status'])
				except:
					detailed_status = 'NULL'
				try:
					dewpoint = str(weather['dewpoint'])
				except:
					dewpoint = 'NULL'
				try:
					heat_index = str(weather['heat_index'])
				except:
					heat_index = 'NULL'
				try:
					humidex = str(weather['humidex'])
				except:
					humidex = 'NULL'
				try:
					humidity = str(weather['humidity'])
				except:
					humidity = 'NULL'
				try:
					pressure_pres = str(weather['pressure']['press'])
				except:
					pressure_pres = 'NULL'
				try:
					pressure_sea_level = str(weather['pressure']['sea_level'])
				except:
					pressure_sea_level = 'NULL'
				try:
					rain = str(weather['rain']['3h'])
				except:
					rain = 'NULL'
				try:
					reference_time = datetime.datetime.fromtimestamp(int(weather['reference_time'])).strftime('%Y-%m-%d %H:%M:%S')
				except:
					reference_time = 'NULL'
				try:
					snow = str(weather['snow']['3h'])
				except:
					snow = 'NULL'
				try:
					status = str(weather['status'])
				except:
					status = 'NULL'
				try:
					sunrise_time = datetime.datetime.fromtimestamp(int(weather['sunrise_time'])).strftime('%Y-%m-%d %H:%M:%S')
				except:
					sunrise_time = 'NULL'
				try:
					sunset_time = datetime.datetime.fromtimestamp(int(weather['sunset_time'])).strftime('%Y-%m-%d %H:%M:%S')
				except:
					sunset_time = 'NULL'
				try:
					temperature_temp = str(weather['temperature']['temp'])
				except:
					temperature_temp = 'NULL'
				try:
					temperature_temp_kf = str(weather['temperature']['temp_kf'])
				except:
					temperature_temp_kf = 'NULL'
				try: 
					temperature_temp_max = str(weather['temperature']['temp_max'])
				except: 
					temperature_temp_max = 'NULL'
				try:
					temperature_temp_min = str(weather['temperature']['temp_min'])
				except:
					temperature_temp_min = 'NULL'
				try:
					visibility_dist = str(weather['visibility_distance'])
				except:
					visibility_dist = 'NULL'
				try:
					weather_code = str(weather['weather_code'])
				except:
					weather_code = 'NULL'
				try:
					wind_deg = str(weather['wind']['deg'])
				except:
					wind_deg = 'NULL'
				try:
					wind_speed = str(weather['wind']['speed'])
				except:
					wind_speed = 'NULL'

				w_tuple = (reception_time, location, clouds, detailed_status, dewpoint, heat_index, humidex, humidity, pressure_pres, pressure_sea_level, rain, reference_time, snow, status, sunrise_time, sunset_time, temperature_temp, temperature_temp_kf, temperature_temp_max, temperature_temp_min, visibility_dist, weather_code, wind_deg, wind_speed)

				w_list = list(w_tuple)
				for i, r in enumerate(w_list):
					if r == 'None' or r == '' or r == None:
						w_list[i] = 'NULL'

				w_tuple = tuple(w_list)
				to_db.append(w_tuple)
			except:
				pass

		if len(to_db) > 0:
			cursor = con.cursor()
			cursor.executemany("INSERT INTO weather_current (reception_time, location, clouds, detailed_status, dewpoint, heat_index, humidex, humidity, pressure_pres, pressure_sea_level, rain, reference_time, snow, status, sunrise_time, sunset_time, temperature_temp, temperature_temp_kf, temperature_temp_max, temperature_temp_min, visibility_dist, weather_code, wind_deg, wind_speed) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", to_db)
			cursor.close()

			print "\t\tweather_current - Records written: " + str(len(to_db))


def parse_forecast_weather(w_file, con, col_names, values):
	with open('to_parse_weather/'+w_file, 'r') as jsonfile:
		content = jsonfile.read().split('\n\n')

		to_db = []

		for c in content[:-1]:
			try:
				raw = simplejson.loads(c)

				try:
					reception_time = datetime.datetime.fromtimestamp(int(raw['reception_time'])).strftime('%Y-%m-%d %H:%M:%S')
				except:
					reception_time = 'NULL'
				try:
					location = raw['Location']['name']
				except:
					location = 'NULL'

				w_tuple = (reception_time, location)

				try:
					weathers = raw['weathers']
				except:
					weathers = []

				for i, weather in enumerate(weathers):
					try:
						clouds = str(weather['clouds'])
					except:
						clouds = 'NULL'
					try:
						detailed_status = str(weather['detailed_status'])
					except:
						detailed_status = 'NULL'
					try:
						dewpoint = str(weather['dewpoint'])
					except:
						dewpoint = 'NULL'
					try:
						heat_index = str(weather['heat_index'])
					except:
						heat_index = 'NULL'
					try:
						humidex = str(weather['humidex'])
					except:
						humidex = 'NULL'
					try:
						humidity = str(weather['humidity'])
					except:
						humidity = 'NULL'
					try:
						pressure_pres = str(weather['pressure']['press'])
					except:
						pressure_pres = 'NULL'
					try:
						pressure_sea_level = str(weather['pressure']['sea_level'])
					except:
						pressure_sea_level = 'NULL'
					try:
						rain = str(weather['rain']['3h'])
					except:
						rain = 'NULL'
					try:
						reference_time = datetime.datetime.fromtimestamp(int(weather['reference_time'])).strftime('%Y-%m-%d %H:%M:%S')
					except:
						reference_time = 'NULL'
					try:
						snow = str(weather['snow']['3h'])
					except:
						snow = 'NULL'
					try:
						status = str(weather['status'])
					except:
						status = 'NULL'
					try:
						sunrise_time = datetime.datetime.fromtimestamp(int(weather['sunrise_time'])).strftime('%Y-%m-%d %H:%M:%S')
					except:
						sunrise_time = 'NULL'
					try:
						sunset_time = datetime.datetime.fromtimestamp(int(weather['sunset_time'])).strftime('%Y-%m-%d %H:%M:%S')
					except:
						sunset_time = 'NULL'
					try:
						temperature_temp = str(weather['temperature']['temp'])
					except:
						temperature_temp = 'NULL'
					try:
						temperature_temp_kf = str(weather['temperature']['temp_kf'])
					except:
						temperature_temp_kf = 'NULL'
					try: 
						temperature_temp_max = str(weather['temperature']['temp_max'])
					except: 
						temperature_temp_max = 'NULL'
					try:
						temperature_temp_min = str(weather['temperature']['temp_min'])
					except:
						temperature_temp_min = 'NULL'
					try:
						visibility_dist = str(weather['visibility_distance'])
					except:
						visibility_dist = 'NULL'
					try:
						weather_code = str(weather['weather_code'])
					except:
						weather_code = 'NULL'
					try:
						wind_deg = str(weather['wind']['deg'])
					except:
						wind_deg = 'NULL'
					try:
						wind_speed = str(weather['wind']['speed'])
					except:
						wind_speed = 'NULL'

					w_tuple = w_tuple + (clouds, detailed_status, dewpoint, heat_index, humidex, humidity, pressure_pres, pressure_sea_level, rain, reference_time, snow, status, sunrise_time, sunset_time, temperature_temp, temperature_temp_kf, temperature_temp_max, temperature_temp_min, visibility_dist, weather_code, wind_deg, wind_speed)

				w_list = list(w_tuple)
				for i, r in enumerate(w_list):
					if r == 'None' or r == '' or r == None:
						w_list[i] = 'NULL'

				w_tuple = tuple(w_list)

				if len(w_tuple) < 882:
					filler = tuple(('NULL' for i in range(882 - len(w_tuple))))
					w_tuple += filler

				if len(w_tuple) > 882:
					w_tuple = w_tuple[:882]

				to_db.append(w_tuple)

			except:
				pass
		if len(to_db) > 0:

			cursor = con.cursor()
			cursor.executemany("INSERT INTO weather_forecast " + col_names + " VALUES " + values, to_db)
			cursor.close()
			print "\t\tweather_forecast - Records written: " + str(len(to_db))



def main(w_file, col_names, values):

	con = MySQLdb.connect("localhost", mysql_user, mysql_passowrd, mysql_database)

	ind = (re.search('_(.*)_.*_.*.txt', w_file)).group(1)
	
	if ind == 'current':
		parse_current_weather(w_file, con)
	elif ind == 'forecast':
		parse_forecast_weather(w_file, con, col_names, values)
	else:
		print 'Error: file ' + w_file + ' not recognized as current or forecast.'

	con.commit()

	get_record_counts(con)

	con.close()

if __name__ == "__main__":
	pass
