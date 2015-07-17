import datetime
import MySQLdb
from mysql_cred import mysql_user, mysql_passowrd, mysql_database

con = MySQLdb.connect("localhost", mysql_user, mysql_passowrd, mysql_database)

with open('../data_collection/load.txt', 'r') as readfile:
	content = readfile.read().split('\n')

	to_db = []

	for i, c in enumerate(content[:-1]):

		raw = c.split('\t')

		sched_num = raw[0]
		station = raw[1]
		route = raw[2]
		train = raw[3]
		passload = raw[4]

		to_db.append((sched_num, station, route, train, passload))


	cursor = con.cursor()
	cursor.executemany("""INSERT INTO sched_load (sched_num, station, route, train, passload) VALUES (%s, %s, %s, %s, %s)""", to_db)
	cursor.close()
	print "\t\tsched_load - Records written: " + str(len(to_db))




with open('../data_collection/routesched.txt', 'r') as readfile:
	content = readfile.read().split('\n')

	to_db = []

	for i, c in enumerate(content[:-1]):

		raw = c.split('\t')

		sched_num = raw[0]
		day = raw[1]
		route = raw[2]
		train = raw[3]
		station = raw[4]
		origtime = raw[5]
		bikeflag = raw[6]

		to_db.append((sched_num, day, route, train, station, origtime, bikeflag))


	cursor = con.cursor()
	cursor.executemany("""INSERT INTO sched_routesched (sched_num, day, route, train, station, origtime, bikeflag) VALUES (%s, %s, %s, %s, %s, %s, %s)""", to_db)
	cursor.close()
	print "\t\tsched_routesched - Records written: " + str(len(to_db))


con.commit()
con.close()