import MySQLdb
from mysql_cred import mysql_user, mysql_passowrd, mysql_database
import sys

def drop_bart(cursor):
	#drop bart tables 
	cursor.execute("""drop table if exists adv_bsa;""")
	cursor.execute("""drop table if exists adv_count;""")
	cursor.execute("""drop table if exists adv_elev;""")
	cursor.execute("""drop table if exists rout_routes;""")
	cursor.execute("""drop table if exists rout_routeinfo;""")
	cursor.execute("""drop table if exists rt_etd;""")
	cursor.execute("""drop table if exists sched_special;""")


def create_bart(cursor):
	#create all bart tables
	cursor.execute("""CREATE TABLE if not exists adv_bsa (
	       uid bigint NOT NULL AUTO_INCREMENT, 
	       fetchtime varchar(26), 
	       date varchar(10), 
	       time varchar(15), 
	       id varchar(7), 
	       station varchar(4), 
	       type varchar(9), 
	       description text, 
	       posted varchar(28), 
	       expires varchar(28),
	       PRIMARY KEY (uid))""")
	result = cursor.fetchall()
	#print result

	cursor.execute("""CREATE TABLE if not exists adv_count (
	       uid bigint NOT NULL AUTO_INCREMENT, 
	       fetchtime varchar(26), 
	       date varchar(10), 
	       time varchar(15), 
	       traincount smallint,
	       PRIMARY KEY (uid))""")
	result = cursor.fetchall()
	#print result

	cursor.execute("""CREATE TABLE if not exists adv_elev (
	       uid bigint NOT NULL AUTO_INCREMENT, 
	       fetchtime varchar(26), 
	       date varchar(10), 
	       time varchar(15), 
	       id varchar(7), 
	       station varchar(4), 
	       type varchar(9), 
	       description text, 
	       posted varchar(28), 
	       expires varchar(28),
	       PRIMARY KEY (uid))""")
	result = cursor.fetchall()
	#print result

	cursor.execute("""CREATE TABLE if not exists sched_special (
	       uid bigint NOT NULL AUTO_INCREMENT, 
	       fetchtime varchar(26), 
	       start_date varchar(10), 
	       end_date varchar(10), 
	       start_time varchar(5), 
	       end_time varchar(5), 
	       text_ text, 
	       link text,
	       orig varchar(4),
	       dest varchar(4),
	       day_of_week varchar(20),
	       routes_affected text,
	       PRIMARY KEY (uid))""")
	result = cursor.fetchall()
	#print result

	cursor.execute("""CREATE TABLE if not exists rout_routes (
	       uid bigint NOT NULL AUTO_INCREMENT, 
	       fetchtime varchar(26), 
	       sched_num smallint,
	       name text,
	       abbr varchar(9),
	       routeid varchar(8),
	       number tinyint,
	       color varchar(7),
	       PRIMARY KEY (uid))""")
	result = cursor.fetchall()
	#print result

	cursor.execute("""CREATE TABLE if not exists rout_routeinfo (
	       uid bigint NOT NULL AUTO_INCREMENT, 
	       fetchtime varchar(26),
	       sched_num smallint, 
	       name text,
	       abbr varchar(9),
	       routeid varchar(8),
	       number tinyint,
	       origin varchar(4),
	       destination varchar(4),
	       direction varchar(5),
	       color varchar(7),
	       holidays tinyint,
	       num_stns tinyint,
	       stations text,
	       PRIMARY KEY (uid))""")
	result = cursor.fetchall()
	#print result

	cursor.execute("""CREATE TABLE if not exists rt_etd (
	       uid bigint NOT NULL AUTO_INCREMENT, 
	       fetchtime varchar(26),
	       date varchar(10),
	       time varchar(15),
	       name varchar(40),
	       abbr varchar(4),
	       destination varchar(40),
	       abbreviation varchar(4),
	       minutes varchar(10),
	       platform tinyint,
	       direction varchar(5),
	       length tinyint,
	       color varchar(10),
	       hexcolor varchar(7),
	       bikeflag tinyint,
	       PRIMARY KEY (uid))""")
	result = cursor.fetchall()
	#print result

	cursor.execute("""show tables;""")
	result = cursor.fetchall()
	print result

def drop_weather(cursor):
	#drop weather tables 
	cursor.execute("""drop table if exists weather_current;""")
	cursor.execute("""drop table if exists weather_forecast;""")


def create_weather(cursor):
	#create all bart tables
	cursor.execute("""CREATE TABLE if not exists weather_current (
		uid bigint NOT NULL AUTO_INCREMENT,
		reception_time varchar(19),
		location varchar(21),
		clouds smallint,
		detailed_status varchar(30),
		dewpoint varchar(5),
		heat_index varchar(4),
		humidex varchar(4),
		humidity smallint,
		pressure_pres float,
		pressure_sea_level float,
		rain float,
		reference_time varchar(19),
		snow float,
		status varchar(30),
		sunrise_time varchar(19),
		sunset_time varchar(19),
		temperature_temp float,
		temperature_temp_kf float,
		temperature_temp_max float,
		temperature_temp_min float,
		visibility_dist mediumint,
		weather_code smallint,
		wind_deg float,
		wind_speed float,
		PRIMARY KEY (uid))""")
	result = cursor.fetchall()
	#print result


	forecast_cols = ["clouds", "detailed_status", "dewpoint", "heat_index", "humidex", "humidity", "pressure_pres", "pressure_sea_level", "rain", "reference_time", "snow", "status", "sunrise_time", "sunset_time", "temperature_temp", "temperature_temp_kf", "temperature_temp_max", "temperature_temp_min", "visibility_dist", "weather_code", "wind_deg", "wind_speed"]

	c_types = ["smallint," ,"varchar(30)," ,"varchar(5)," ,"varchar(4)," ,"varchar(4)," ,"smallint," ,"float," ,"float," ,"float," ,"varchar(19)," ,"float," ,"varchar(30)," ,"varchar(19)," ,"varchar(19)," ,"float," ,"float," ,"float," ,"float," ,"mediumint," ,"smallint," ,"float," ,"float,"]

	col_names = ['reception_time', 'location']
	for i in range(40):
		col_names += [c+"_"+str((i+1)*3) for c in forecast_cols]

	col_types = ["varchar(19),", "varchar(21),"]
	for i in range(40):
		col_types += c_types

	weather_forecast_query = ""
	for a, b in zip(col_names, col_types):
		weather_forecast_query += a + " " + b + " \n"

	weather_forecast_query = "CREATE TABLE if not exists weather_forecast (uid bigint NOT NULL AUTO_INCREMENT, " + weather_forecast_query + "PRIMARY KEY (uid))"

	cursor.execute(weather_forecast_query)
	result = cursor.fetchall()
	#print result

	cursor.execute("""show tables;""")
	result = cursor.fetchall()
	print result

def drop_onetime(cursor):
	#drop weather tables 
	cursor.execute("""drop table if exists sched_routesched;""")
	cursor.execute("""drop table if exists sched_load;""")


def create_onetime(cursor):
	cursor.execute("""CREATE TABLE if not exists sched_routesched (
		uid bigint NOT NULL AUTO_INCREMENT, 
		sched_num tinyint,
		day varchar(2), 
		route tinyint, 
		train tinyint, 
		station varchar(4), 
		origtime varchar(7), 
		bikeflag tinyint,
		PRIMARY KEY (uid))""")
	result = cursor.fetchall()
	#print result

	cursor.execute("""CREATE TABLE if not exists sched_load (
		uid bigint NOT NULL AUTO_INCREMENT, 
		sched_num tinyint,
		station varchar(4),
		route tinyint, 
		train tinyint,
		passload tinyint,
		PRIMARY KEY (uid))""")
	result = cursor.fetchall()
	#print result

	cursor.execute("""show tables;""")
	result = cursor.fetchall()
	print result


if __name__ == "__main__":

	con = MySQLdb.connect("localhost", mysql_user, mysql_passowrd, mysql_database)
	cursor = con.cursor()

	if sys.argv[1] == "drop_bart":
		drop_bart(cursor)
	elif sys.argv[1] == "create_bart":
		create_bart(cursor)
	elif sys.argv[1] == "drop_weather":
		drop_weather(cursor)
	elif sys.argv[1] == "create_weather":
		create_weather(cursor)
	elif sys.argv[1] == "drop_onetime":
		drop_onetime(cursor)
	elif sys.argv[1] == "create_onetime":
		create_onetime(cursor)
	else:
		print "You need to specify an option: drop_bart, create_bart, drop_weather, create_weather"

	con.close()

