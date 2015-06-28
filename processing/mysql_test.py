from pandas.io import sql
import pandas as pd
import MySQLdb
from mysql_cred import mysql_user, mysql_passowrd


con = MySQLdb.connect("localhost", mysql_user, mysql_passowrd, "test")


cursor = con.cursor()

cursor.execute("""show databases;""")
result = cursor.fetchall()
print result

cursor.execute("""CREATE TABLE adv_bsa (
       uid MEDIUMINT NOT NULL AUTO_INCREMENT, date varchar(10), time varchar(15), id varchar(7), 
	station varchar(4), type varchar(9), description text, posted varchar(28), expires varchar(28),
       PRIMARY KEY (uid))""")
result = cursor.fetchall()
print result

cursor.execute("""show tables;""")
result = cursor.fetchall()
print result

cursor.execute("""show create table adv_bsa;""")
result = cursor.fetchall()
print result
#cursor.execute("""SELECT spam, eggs, sausage FROM breakfast WHERE price < %s""", (max_price,))


#df.to_sql(con = con, name = 'test_table', if_exists = 'append', flavor = 'mysql')
