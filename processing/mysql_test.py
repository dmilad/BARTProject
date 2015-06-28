from pandas.io import sql
import pandas as pd
import MySQLdb
from mysql_cred import mysql_user, mysql_passowrd


con = MySQLdb.connect("localhost", mysql_user, mysql_passowrd, "test")


cursor = con.cursor()

#cursor.execute("""SELECT spam, eggs, sausage FROM breakfast WHERE price < %s""", (max_price,))


#df.to_sql(con = con, name = 'test_table', if_exists = 'append', flavor = 'mysql')