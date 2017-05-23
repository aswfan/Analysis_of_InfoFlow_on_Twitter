import mysql.connector
import mysql.connector.pooling

class MySQLConnector:
	cnxpool = None
	@staticmethod
	def configConnectionPool(
		host = 'db.imyyfan.com', 
		database = 'infx547', 
		username = 'yyfan', 
		password = '12345678'
		):
		dbconfig = {
		'host': host,
		'database': database,
		'user': username,
		'password': password
		}

		MySQLConnector.cnxpool = mysql.connector.pooling.MySQLConnectionPool(
			pool_name = 'mypool',
			pool_size = 3,
			**dbconfig)
	@staticmethod
	def getConnection():
		if MySQLConnector.cnxpool == None:
			MySQLConnector.configConnectionPool()
		return MySQLConnector.cnxpool.get_connection()

if __name__ == '__main__':
	host = 'db.imyyfan.com'
	database = 'infx547'
	username = 'yyfan'
	password = '12345678'

	MySQLConnector.configConnectionPool(
		host, database, username, password)
	cnx = MySQLConnector.getConnection()
	if cnx != None:
		print 'Successful'
	else:
		print "failed"



