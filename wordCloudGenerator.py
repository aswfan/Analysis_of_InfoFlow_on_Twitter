from ConnectionPool import MySQLConnector as pool
import json

cnn = pool.getConnection()
sql = 'select text from status'

try:
	cursor = cnn.cursor(buffered = true)
	cursor.execute(sql)
	cnn.commit()
	result = cursor.fetchall()
except mysql.connector.Error as err:
	print indent_2 + 'Somthing went wrong: {}'.format(err)
	cnn.rollback()
	cnn.close()
	raise

with open('test.txt', 'w') as f:
    for row in result:
        print row
        f.write("%s\n" % str(row))