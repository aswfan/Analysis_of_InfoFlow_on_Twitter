import mysql.connector
from ConnectionPool import MySQLConnector as pool
import json

if __name__ == '__main__':

	cnn = pool.getConnection()

	# cnn = mysql.connector.connect(user='root', password='123456',
	# 							host='localhost',
	# 							database='testforinfx547')
	

	cursor = cnn.cursor(buffered = True)
	# Wed May 24 06:10:56 +0000 2017

	# format time
	# ss = '+0000 '
	# select_sql = "select id, created_at from user where created_at is not null AND time is null"
	# try:
	# 	cursor.execute(select_sql)
	# 	cnn.commit()
	# 	results = cursor.fetchall()
	# except mysql.connector.Error as err:
	# 	print 'Somthing went wrong in querying user: {}'.format(err)
	# 	print ''
	# 	print select_sql
	# 	cnn.rollback()
	# 	cnn.close()
	# 	raise
	# print "Successfully obtain all data"

	# print ''
	# print "Attempting to format time..."
	# size = len(results)
	# i = 1
	# for result in results:
	# 	print 'Progress:' + '{0:.2f}'.format(i * 100.0 / size) + '%'
	# 	sid = result[0]
	# 	date = result[1]
	# 	update_sql = "update user set time = STR_TO_DATE('{}', '%W %M %e %T %Y') where id = '{}'"
	# 	if ss not in date:
	# 		print date + ' do not have +0000!'
	# 	date = date.replace(ss, '')
	# 	try:
	# 		cursor.execute(update_sql.format(date, sid))
	# 		cnn.commit()
	# 	except mysql.connector.Error as err:
	# 		print 'Somthing went wrong: {}'.format(err)
	# 		print ''
	# 		print update_sql
	# 		cnn.rollback()
	# 		cnn.close()
	# 		raise
	# 	i += 1
	# cnn.close()

# format date
	select_sql = "select id, DATE_FORMAT(time,'%Y-%m-%d') from status where time is not null"
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
	print "Successfully fetch all data!"

	print ''
	print "Attempting to format date..."
	size = len(results)
	i = 1
	for result in results:
		print 'Progress:' + '{0:.2f}'.format(i * 100.0 / size) + '%'
		sid = result[0]
		time = result[1]
		update_sql = "update status set date = '{}' where id = '{}'"
		# print update_sql.format(time, sid)
		try:
			cursor.execute(update_sql.format(time, sid))
			cnn.commit()
		except mysql.connector.Error as err:
			print 'Somthing went wrong: {}'.format(err)
			print ''
			print update_sql.format(time, sid)
			cnn.rollback()
			cnn.close()
			raise
		i += 1
	cnn.close()
