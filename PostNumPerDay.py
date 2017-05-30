import mysql.connector
from ConnectionPool import MySQLConnector as pool
import json
import csv

if __name__ == '__main__':

	cnn = pool.getConnection()

	cursor = cnn.cursor(buffered = True)

	select_sql = "select distinct DATE_FORMAT(date,'%e-%b-%y'), count(*) from infx547.status where date is not null group by date"
	try:
		cursor.execute(select_sql)
		cnn.commit()
		results = cursor.fetchall()
	except mysql.connector.Error as err:
		print 'Somthing went wrong in querying user: {}'.format(err)
		print ''
		print select_sql
		cnn.rollback()
		cnn.close()
		raise
	print "Successfully obtain all data"

	with open('numpostperdat.csv', 'wb') as file:
		wr = csv.writer(file, quoting=csv.QUOTE_ALL)
		wr.writerow(['date', 'num'])
		for result in results:
			wr.writerow(result)