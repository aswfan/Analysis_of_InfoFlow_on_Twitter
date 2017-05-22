
# coding: utf-8

# In[1]:

import pandas as pd
pd.__version__


# In[3]:

# REST API
import sys
import json
import pandas as pd

#Fill in your twitter dev info.
consumer_key = '9qfh9E7NytEJir0x8DPhA0Wo3'
consumer_secret = 'KXrazhottbw0htNyaz8ghC7JiPvmbuunqmDToqp1fGoqX2Cwa0'

access_token ='775111751108923392-0i4ZTsWb11OgIBSI9ZoA7noTLdUwSQI'
access_token_secret = 'prMoEZkGgvgOaWwI9WOlHuaNr4D6jkQPhZkWkh8ZPuxJo'

import twitter
api = twitter.Api(consumer_key, consumer_secret, access_token, access_token_secret)
# print api.VerifyCredentials()

import time
now = time.strftime("%c")

query = "q=book&result_type=recent&count=1"

data = api.GetSearch(raw_query= query)

print data


# In[18]:

result = {}
result['result'] = []
for item in data:
    result['result'].append(json.loads(item.AsJsonString()))
print json.dumps(result, indent=4)

with open('preforERD.json', 'w') as file:
    json.dump(result, file)
    
    print('data saved successfully!')


# In[40]:

import json

with open('preforERD.json', 'r') as file:
    data = json.load(file)
print json.dumps(data['result'], indent=4)


# In[32]:

# export to excel
import pandas as pd

with open('preforERD.json', 'r') as file:
    pd_data = pd.read_json(json.dumps(data['result']))

    print pd_data

pd_data.to_csv('test.csv')


# In[42]:

print json.dumps(data['result'][0]['media'], indent=4)


# In[41]:

print json.dumps(data['result'][0], indent=4)


# In[57]:

for item in data['result'][0]['retweeted_status']['user']:
    if item in data['result'][0]['user']:
        continue
    print item
    


# In[4]:

result = {}
result['result'] = []
for item in data:
    result['result'].append(json.loads(item.AsJsonString()))
print json.dumps(result, indent=4)


# In[ ]:



