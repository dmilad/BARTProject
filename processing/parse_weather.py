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

			raw = simplejson.loads(c)

			reception_time = datetime.datetime.fromtimestamp(int(raw['reception_time'])).strftime('%Y-%m-%d %H:%M:%S')
			location = raw['Location']['name']

			weather = raw['Weather']

			clouds = str(weather['clouds'])
			detailed_status = str(weather['detailed_status'])
			dewpoint = str(weather['dewpoint'])
			heat_index = str(weather['heat_index'])
			humidex = str(weather['humidex'])
			humidity = str(weather['humidity'])
			pressure_pres = str(weather['pressure']['press'])
			pressure_sea_level = str(weather['pressure']['sea_level'])
			try:
				rain = str(weather['rain']['3h'])
			except:
				rain = 'NULL'
			reference_time = datetime.datetime.fromtimestamp(int(weather['reference_time'])).strftime('%Y-%m-%d %H:%M:%S')
			try:
				snow = str(weather['snow']['3h'])
			except:
				snow = 'NULL'
			status = str(weather['status'])
			sunrise_time = datetime.datetime.fromtimestamp(int(weather['sunrise_time'])).strftime('%Y-%m-%d %H:%M:%S')
			sunset_time = datetime.datetime.fromtimestamp(int(weather['sunset_time'])).strftime('%Y-%m-%d %H:%M:%S')
			temperature_temp = str(weather['temperature']['temp'])
			temperature_temp_kf = str(weather['temperature']['temp_kf'])
			temperature_temp_max = str(weather['temperature']['temp_max'])
			temperature_temp_min = str(weather['temperature']['temp_min'])
			visibility_dist = str(weather['visibility_distance'])
			weather_code = str(weather['weather_code'])
			wind_deg = str(weather['wind']['deg'])
			wind_speed = str(weather['wind']['speed'])

			w_tuple = (reception_time, location, clouds, detailed_status, dewpoint, heat_index, humidex, humidity, pressure_pres, pressure_sea_level, rain, reference_time, snow, status, sunrise_time, sunset_time, temperature_temp, temperature_temp_kf, temperature_temp_max, temperature_temp_min, visibility_dist, weather_code, wind_deg, wind_speed)

			w_list = list(w_tuple)
			for i, r in enumerate(w_list):
				if r == 'None' or r == '':
					w_list[i] = 'NULL'

			w_tuple = tuple(w_list)
			to_db.append(w_tuple)

		cursor = con.cursor()
		cursor.executemany("INSERT INTO weather_current (reception_time, location, clouds, detailed_status, dewpoint, heat_index, humidex, humidity, pressure_pres, pressure_sea_level, rain, reference_time, snow, status, sunrise_time, sunset_time, temperature_temp, temperature_temp_kf, temperature_temp_max, temperature_temp_min, visibility_dist, weather_code, wind_deg, wind_speed) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", to_db)
		cursor.close()

		print "\t\tweather_current - Records written: " + str(len(to_db))


def parse_forecast_weather(w_file, con, col_names, values):
	with open('to_parse_weather/'+w_file, 'r') as jsonfile:
		content = jsonfile.read().split('\n\n')

		to_db = []

		for c in content[:-1]:
			raw = simplejson.loads(c)

			reception_time = datetime.datetime.fromtimestamp(int(raw['reception_time'])).strftime('%Y-%m-%d %H:%M:%S')
			location = raw['Location']['name']

			w_tuple = (reception_time, location)

			weathers = raw['weathers']

			for i, weather in enumerate(weathers):
				clouds = str(weather['clouds'])
				detailed_status = str(weather['detailed_status'])
				dewpoint = str(weather['dewpoint'])
				heat_index = str(weather['heat_index'])
				humidex = str(weather['humidex'])
				humidity = str(weather['humidity'])
				pressure_pres = str(weather['pressure']['press'])
				pressure_sea_level = str(weather['pressure']['sea_level'])
				try:
					rain = str(weather['rain']['3h'])
				except:
					rain = 'NULL'
				reference_time = datetime.datetime.fromtimestamp(int(weather['reference_time'])).strftime('%Y-%m-%d %H:%M:%S')
				try:
					snow = str(weather['snow']['3h'])
				except:
					snow = 'NULL'
				status = str(weather['status'])
				sunrise_time = datetime.datetime.fromtimestamp(int(weather['sunrise_time'])).strftime('%Y-%m-%d %H:%M:%S')
				sunset_time = datetime.datetime.fromtimestamp(int(weather['sunset_time'])).strftime('%Y-%m-%d %H:%M:%S')
				temperature_temp = str(weather['temperature']['temp'])
				temperature_temp_kf = str(weather['temperature']['temp_kf'])
				temperature_temp_max = str(weather['temperature']['temp_max'])
				temperature_temp_min = str(weather['temperature']['temp_min'])
				visibility_dist = str(weather['visibility_distance'])
				weather_code = str(weather['weather_code'])
				wind_deg = str(weather['wind']['deg'])
				wind_speed = str(weather['wind']['speed'])

				w_tuple = w_tuple + (clouds, detailed_status, dewpoint, heat_index, humidex, humidity, pressure_pres, pressure_sea_level, rain, reference_time, snow, status, sunrise_time, sunset_time, temperature_temp, temperature_temp_kf, temperature_temp_max, temperature_temp_min, visibility_dist, weather_code, wind_deg, wind_speed)

			w_list = list(w_tuple)
			for i, r in enumerate(w_list):
				if r == 'None' or r == '':
					w_list[i] = 'NULL'

			w_tuple = tuple(w_list)

			if len(w_tuple) < 882:
				filler = tuple(('NULL' for i in range(882 - len(w_tuple))))
				w_tuple += filler

			to_db.append(w_tuple)

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
