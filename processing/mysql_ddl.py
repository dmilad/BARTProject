import MySQLdb
from mysql_cred import mysql_user, mysql_passowrd, mysql_database


con = MySQLdb.connect("localhost", mysql_user, mysql_passowrd, mysql_database)
cursor = con.cursor()

#cursor.execute("""show databases;""")
#result = cursor.fetchall()
#print result




#drop tables 

cursor.execute("""drop table if exists adv_bsa;""")
cursor.execute("""drop table if exists adv_count;""")
cursor.execute("""drop table if exists adv_elev;""")
cursor.execute("""drop table if exists rout_routes;""")
cursor.execute("""drop table if exists rout_routeinfo;""")
cursor.execute("""drop table if exists rt_etd;""")
cursor.execute("""drop table if exists sched_special;""")




#create all tables

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

