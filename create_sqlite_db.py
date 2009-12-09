#!/usr/bin/python
# -*- coding: utf-8 -*-

import sqlite, os
import psycopg2

def ConnectToDB ( dbname="my_db", user="my_user", host="oleiros", password="my_password" ):
	if os.uname()[1]=="kermode":
		host="localhost"
		dsn = "dbname='%s' user='%s' host='%s' password='%s' port='5433'"\
				%(dbname, user, host, password)
	else:
		dsn = "dbname='%s' user='%s' host='%s' password='%s'"\
		%(dbname, user, host, password)
		#dsn = "dbname='%s' user='%s' host='%s' password='%s'"%("firemafs","fire","oleiros","fire")
	try:
		conn = psycopg2.connect(dsn)
	except:
		print "Can't connect to database. Sorry!"
	return conn.cursor() # Get cursor
	
def ParseSchema ( schema ):
	fp = open("%s.schema"%schema, 'r')
	tables=[]
	while True:
		L = fp.readline()
		if not L: break
		if L.find("CREATE TABLE")>=0:
			sql = "" + L
			L = fp.readline()
			while L.find(";")<0:
				sql = sql + L
				L = fp.readline()
			sql = sql + L
			tables.append ( sql )
	return tables

schemas = ['observation', 'drivers', 'lpj_run_0', 'lpj_run_1']
pg_cursor = ConnectToDB()
for schema in schemas:
	print "Schema: ", schema
	conn = sqlite.connect("/tmp/%s.sqlite"%schema)
	conn.autocommit=1
	sqlite_cursor = conn.cursor()
	tables = ParseSchema ( schema )
	for table in tables:
		table_name = table.split()[2]
		print "\t Table:", table_name
		try:
			sqlite_cursor.execute ( table )
		except sqlite.DatabaseError, msg:
			print msg

		pg_cursor.execute ("select * from %s.%s"%(schema,table_name))
		print "\t Inserting data..."
		for row in pg_cursor.fetchall():
			sqlite_cursor.execute("INSERT INTO %s values "%table_name + str(row))
		conn.commit()
	conn.close()