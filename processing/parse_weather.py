import simplejson
import datetime
import MySQLdb
from mysql_cred import mysql_user, mysql_passowrd, mysql_database



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

			clouds = weather['clouds']
			detailed_status = weather['detailed_status']
			dewpoint = weather['dewpoint']
			heat_index = weather['heat_index']
			humidex = weather['humidex']
			humidity = weather['humidity']
			pressure_pres = weather['pressure']['press']
			pressure_sea_level = weather['pressure']['sea_level']
			try:
				rain = weather['rain']['3h']
			except:
				rain = 'None'
			reference_time = datetime.datetime.fromtimestamp(int(weather['reference_time'])).strftime('%Y-%m-%d %H:%M:%S')
			try:
				snow = weather['snow']['3h']
			except:
				snow = 'None'
			status = weather['status']
			sunrise_time = datetime.datetime.fromtimestamp(int(weather['sunrise_time'])).strftime('%Y-%m-%d %H:%M:%S')
			sunset_time = datetime.datetime.fromtimestamp(int(weather['sunset_time'])).strftime('%Y-%m-%d %H:%M:%S')
			temperature_temp = weather['temperature']['temp']
			temperature_temp_kf = weather['temperature']['temp_kf']
			temperature_temp_max = weather['temperature']['temp_max']
			temperature_temp_min = weather['temperature']['temp_min']
			visibility_dist = weather['visibility_distance']
			weather_code = weather['weather_code']
			wind_deg = weather['wind']['deg']
			wind_speed = weather['wind']['speed']

			to_db.append((reception_time, location, clouds, detailed_status, dewpoint, heat_index, humidex, humidity, pressure_pres, pressure_sea_level, rain, reference_time, snow, status, sunrise_time, sunset_time, temperature_temp, temperature_temp_kf, temperature_temp_max, temperature_temp_min, visibility_dist, weather_code, wind_deg, wind_speed))

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

			w_tuple = ((reception_time, location))

			weathers = raw['weathers']

			for i, weather in enumerate(weathers):
				clouds = weather['clouds']
				detailed_status = weather['detailed_status']
				dewpoint = weather['dewpoint']
				heat_index = weather['heat_index']
				humidex = weather['humidex']
				humidity = weather['humidity']
				pressure_pres = weather['pressure']['press']
				pressure_sea_level = weather['pressure']['sea_level']
				try:
					rain = weather['rain']['3h']
				except:
					rain = 'None'
				reference_time = datetime.datetime.fromtimestamp(int(weather['reference_time'])).strftime('%Y-%m-%d %H:%M:%S')
				snow = str(weather['snow'])
				try:
					snow = weather['snow']['3h']
				except:
					snow = 'None'
				status = weather['status']
				sunrise_time = datetime.datetime.fromtimestamp(int(weather['sunrise_time'])).strftime('%Y-%m-%d %H:%M:%S')
				sunset_time = datetime.datetime.fromtimestamp(int(weather['sunset_time'])).strftime('%Y-%m-%d %H:%M:%S')
				temperature_temp = weather['temperature']['temp']
				temperature_temp_kf = weather['temperature']['temp_kf']
				temperature_temp_max = weather['temperature']['temp_max']
				temperature_temp_min = weather['temperature']['temp_min']
				visibility_dist = weather['visibility_distance']
				weather_code = weather['weather_code']
				wind_deg = weather['wind']['deg']
				wind_speed = weather['wind']['speed']

				w_tuple = w_tuple + (clouds, detailed_status, dewpoint, heat_index, humidex, humidity, pressure_pres, pressure_sea_level, rain, reference_time, snow, status, sunrise_time, sunset_time, temperature_temp, temperature_temp_kf, temperature_temp_max, temperature_temp_min, visibility_dist, weather_code, wind_deg, wind_speed)

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

	get_record_counts()

	con.close()

if __name__ == "__main__":
	pass