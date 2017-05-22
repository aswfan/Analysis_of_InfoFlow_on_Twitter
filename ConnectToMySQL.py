
# coding: utf-8

# In[6]:

import mysql.connector

cnx = mysql.connector.connect(user='yyfan', password='12345678',
                              host='db.imyyfan.com',
                              database='infx547')
cursor = cnx.cursor()


# In[8]:

# select from db

select_q = 'select * from foo'

cursor.execute(select_q)

for var in cursor:
    print var[0]


# In[ ]:

# insert into db

insert_q = 'insert into '


# In[ ]:

cursor.close()

cnx.close()

