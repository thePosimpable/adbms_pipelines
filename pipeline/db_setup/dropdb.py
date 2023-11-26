import mysql.connector, os, sys
from pipeline.config import config
import configparser

def rundrop(mycursor, db):
	sql = "DROP DATABASE IF EXISTS %s" % (db)
	mycursor.execute(sql)

def createdb(mycursor, db):
	sql = "CREATE DATABASE %s" % (db)
	mycursor.execute(sql)

def parseSQL(path, filename):
	sqlpath = f"{path}{filename}"

	with open(sqlpath, 'r') as file:
		data = file.read()

	return data

def runSQL(sql, db):
	# print("runSQL")

	con = mysql.connector.connect(
		host = config.get('MYSQL', 'MYSQL_SERVER'),
		user = config.get('MYSQL', 'MYSQL_USER'),
		password = config.get('MYSQL', 'MYSQL_PASSWORD'),
		database = db
	)

	mysqlcursor = con.cursor()

	result_iterator = mysqlcursor.execute(sql, multi=True)

	for res in result_iterator:
		res
		# print("Running query: ", res)  # Will print out a short representation of the query
		# print(f"Affected {res.rowcount} rows")
	
	con.commit()

def dropdb(config, path, filename, db):
	print("Running dropdb.py")
	print(f"Rebasing {db} database using {path}{filename}.")
	print(f"Parameters:\npath = {path}\nfilename = {filename}\ndb = {db}")

	config.get('MYSQL', 'DHDC_DESTDIR_PATH')
	mydb = mysql.connector.connect(
		host = config.get('MYSQL', 'MYSQL_SERVER'),
		port = config.get('MYSQL', 'MYSQL_POST'),
		user = config.get('MYSQL', 'MYSQL_USER'),
		password = config.get('MYSQL', 'MYSQL_PASSWORD')
	)

	mycursor = mydb.cursor()

	rundrop(mycursor, db)
	createdb(mycursor, db)
	runSQL(parseSQL(path, filename), db)

	print("End dropdb.py\n")