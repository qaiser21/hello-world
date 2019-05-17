#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd


# In[2]:


from pymongo import MongoClient
client = MongoClient()


# In[3]:


MongoClient = require('mongodb').MongoClient;
url = 'mongodb://130.23.9.144:27017/';

MongoClient.connect(url, function(err, db) {
  
}); 


# In[4]:


import pymongo
from pymongo import MongoClient
client = MongoClient()
client = MongoClient("localhost", 27017)
client = MongoClient('mongodb://130.23.9.144:27017/')
mon_db = client.dh

cols = mon_db.list_collection_names()


# In[5]:


cols


# In[6]:


import pymongo
from pymongo import MongoClient
Connection = MongoClient()
mon_con = Connection('mongodb://130.23.9.144', 27017)
client = mon_con('mongodb://130.23.9.144:27017/')
mon_db = mon_con.dh

cols = mon_db.collection_names()
for c in cols:
    print(c)


# In[7]:


import pymongo
db_connect = pymongo.MongoClient('mongodb://130.23.9.144', 27017)
database_name = 'Sources_1'
database = db_connect[database_name]
collection = database.list_collection_names()
for collect in collection:
    print(collect)


# In[ ]:





# In[8]:


from pymongo import MongoClient
client = MongoClient("130.23.9.144", 27017, maxPoolSize=50)
db=client.Sources_1
collection=db['sources_1']
cursor = collection.find({})
for document in cursor:
     print(document)


# In[54]:


from pymongo import MongoClient

client = MongoClient('mongodb://130.23.9.144:27017/')

with client:
    db = client.Sources_1
    cars = db.sources.find()
    for car in cars:
        print('{0} {1}'.format(car['name'], 
            car['price']))


# In[46]:


cars.collection.


# In[107]:


import pymongo
import json

if __name__ == '__main__':
    client = pymongo.MongoClient("mongodb://130.23.9.144:27017/", 27017, maxPoolSize=50)
    d = dict((db, [collection for collection in client[db].collection_names()])
             for db in client.database_names())
    print(json.dumps(d))


# In[108]:


db = client.sources_1


# In[49]:


db.collection_names()


# In[50]:


col =db.uspto


# In[109]:


x =col.find()


# In[110]:


n_cars = x.count()
n_cars


# In[44]:


print("There are {} items".format(n_cars))


# In[151]:


cars = db.uspto1.find({}, {'_id': 1, 'name':2})


# In[36]:


n = cars.count()


# In[153]:


n


# In[157]:


for car in cars:
    print('{0} {1}'.format(car['description'],car['name']))


# In[159]:


for car in cars:
    car.keys


# In[164]:


car.values()


# In[166]:


db.uspto1.find({"_id": ObjectId("5cd01dbd68a8a73aa82c2f24")})


# In[18]:


for i,val in enumerate(x):
    if val ==1:
        print(i)


# In[4]:


# import pymongo
# import json

# if __name__ == '__main__':
#     client = pymongo.MongoClient("mongodb://130.23.9.144:27017/", 27017, maxPoolSize=50)
#     d = dict((db, [collection for collection in client[db].collection_names()])
#              for db in client.database_names())
#     print(json.dumps(d))
    
# db = client.sources_1
# col =db.uspto
# # x =col.find()
# from bson.json_util import dumps
# # data = dumps(col.find_one())
# from bson.json_util import loads
# json_str = dumps(col.find())
# record2 = loads(json_str)
# import pandas as pd
# datadfdf = pd.DataFrame(record2)


# In[106]:


from pymongo import MongoClient
client = MongoClient()


# In[104]:


dd = col.find(limit=50)


# In[5]:



# coding: utf-8

# In[3]:


# -*- coding: utf-8 -*-
"""
Created on Thu May  9 08:05:51 2019

@author: MQA54874
"""

from pymongo import MongoClient
from flashtext import KeywordProcessor


kp = KeywordProcessor()

# conecting through mongodb database
client = MongoClient("mongodb://130.23.9.144:27017")


# listing database name in mongodb
client.list_database_names()


db = client['profile_demo_1']


#collection name present in selected db
db.list_collection_names()


#selecting collection info as it will capture user input

data = list(db['infos'].find())[0]


#getting all the key and value in available in data dictionary

key = [x for x in data.keys()]

# o/p['_id','profile_name', 'db_name', 'col_name','server_path','user','created','modified','modifiedby','daterange','filterType','categories','scoring','fields']
values = [x for x in data.values()]




# data categories contain catname(Keyword category, field -- columns that need to searched, keywords --list of keywords for particular category)


dict_1 = data['categories'][0]  
print(dict_1)

keys_1 = [x for x in dict_1.keys()]
# catname = dict_1['catname']
# fields = dict_1['fields']
# keywords = dict_1['keywords']


catname = []
fields = []
keywords = []
    
for i in range(len(data['categories'])):
    dict_1 = data['categories'][i]
    
    catname.append(dict_1['catname'])
    fields.append(dict_1['fields'])
    keywords.append(dict_1['keywords'])
   
    
    


def dict_input(database, collection_name):
    
    data = list(database[collection_name].find())[0]
    
    catname = []
    fields = []
    keywords = []
#     catnamenn = []
    
    for i in range(len(data['categories'])):
        dict_1 = data['categories'][i]

        catname.append(dict_1['catname'])
        fields.append(dict_1['fields'])
        keywords.append(dict_1['keywords'])
       
#         if len(dict_1['fields']) >1: #code added 
#           for l,m in enumerate(range(len(dict_1['fields']))):#code added 
#             catnamenn.append(dict_1['fields'][m]+'_'+dict_1['catname'])#code added 
#         else:
#             catnamenn.append(dict_1['fields'][0]+'_'+dict_1['catname'])#code added 
        
        
    return catname, fields, keywords #code added 
        
    




def kw_processor(catname, keywords):
    
    kp_inst = {}
    for x in catname:
        kp_inst[str(x)]= KeywordProcessor()
    
    kp_keys = [x for x in kp_inst.keys()]
    
    for i in range(len(kp_keys)):
        kp_inst[kp_keys[i]].add_keywords_from_list(keywords[i])
    
    assert len(kp_keys) == len(keywords)
    
    return kp_inst
    
    

def data_processing(df, kp_inst, fields):
    
    keys = [x for x in  kp_inst.keys()]
    
    assert len(fields) == len(keys)
    
    for i in range(len(fields)):
        for colname in fields[i]:
            df[colname + '_'  + keys[i]] = df[colname].apply(lambda x : len(kp_inst[keys[i]].extract_keywords(x)))
            
    return df

# d = data_processing(df1,a,['title', 'Description'] )





# df11 = df1[['title', 'Description']]

# ad=data_processing(df123, dc,  [['title', 'Description'], ['title']])




def scoring(df, score, query_string, class_name):
    
    def scoring_(x, score):
        if x==True:
            return score
        else:
            return 0
    
    df[class_name] = df.eval(query_string)   
    df[class_name] =df[class_name].apply(lambda x: scoring_(x, score)) 
    
    return df


#xv = scoring(asd, 100, '(title_NRK1>0) | (Description_NRK1 >0)', 'cl1')


# In[10]:



# catname, fields, keywords=dict_input(db, 'infos')
# kp_inst=kw_processor(catname, keywords)
af1=data_processing(dummydfdf, kp_inst, fields)

# df, score, query_string, class_name
#scoring(d12345, 50, '(title_NRK1>0) | (Description_NRK1 >0)', 'score')
#af1=data_processing(df, kp_inst, fields)


# In[12]:


scoring(af1, 50, '(title_NRK1>0) | (Description_NRK1 >0)', 'score')


# In[58]:


# kp_inst=kw_processor(catname, keywords)
kp_inst=kw_processor(catname, keywords)
kp_inst


# In[34]:


data = list(db['infos'].find())

data


# In[69]:


af1=data_processing(dummydf, kp_inst, fields)


# In[71]:


scoring(af1, 50, '(title_NRK1>0) | (Description_NRK1 >0)', 'score')


# In[8]:


for i in keywords:
    print(i)


# In[64]:


d12345 = data_processing(dummydf,kp_inst,fields)


# In[67]:


d12345.columns


# In[7]:


import pymongo
import json

if __name__ == '__main__':
    client = pymongo.MongoClient("mongodb://130.23.9.144:27017/", 27017, maxPoolSize=50)
    d = dict((db, [collection for collection in client[db].collection_names()])
             for db in client.database_names())
    print(json.dumps(d))
    
db = client.sources_1
col =db.uspto
# x =col.find()
from bson.json_util import dumps
# data = dumps(col.find_one())
from bson.json_util import loads
json_str = dumps(col.find())
record2 = loads(json_str)
import pandas as pd
uspto = pd.DataFrame(record2)


# In[9]:


dummydfdf = uspto.head()
dummydfdf.shape


# In[98]:


data = list(db['infos'].find())[0]
x = data['scoring']
listapp = []
for i in x:
    listapp.append(i['query'])

# for i,val in enumerate(x):  
#      print(x[val])


# In[103]:


listapp


# In[ ]:


d = data_processing(dummydf,a,['title', 'Description'] )


# In[135]:


data = list(db['infos'].find())[0]
    
catname = []
fields = []
keywords = []
catnamenn = []
for i in range(len(data['categories'])):
   dict_1 = data['categories'][i]
   catname.append(dict_1['catname'])
   if len(dict_1['fields']) >1:
      for l,m in enumerate(range(len(dict_1['fields']))):
            print(dict_1['fields'][m]+'_'+dict_1['catname'])
   else:
            print(dict_1['fields'][0]+'_'+dict_1['catname'])


# In[38]:





# In[39]:


kw_processor(catname,keywords)


# In[5]:


# coding: utf-8

# In[3]:


# -*- coding: utf-8 -*-
"""
Created on Thu May  9 08:05:51 2019

@author: MQA54874
"""

from pymongo import MongoClient
from flashtext import KeywordProcessor


kp = KeywordProcessor()

# conecting through mongodb database
client = MongoClient("mongodb://130.23.9.144:27017")


# listing database name in mongodb
client.list_database_names()


db = client['profile_demo_1']


#collection name present in selected db
db.list_collection_names()

def dict_input(database, collection_name):
    
    data = list(database[collection_name].find())[0]
    
    catname = []
    fields = []
    keywords = []
#     catnamenn = []
    
    for i in range(len(data['categories'])):
        dict_1 = data['categories'][i]

        catname.append(dict_1['catname'])
        fields.append(dict_1['fields'])
        keywords.append(dict_1['keywords'])      

    return catname, fields, keywords #code added 
        
    




def kw_processor(catname, keywords):
    
    kp_inst = {}
    for x in catname:
        kp_inst[str(x)]= KeywordProcessor()
    
    kp_keys = [x for x in kp_inst.keys()]
    
    for i in range(len(kp_keys)):
        kp_inst[kp_keys[i]].add_keywords_from_list(keywords[i])
    
    assert len(kp_keys) == len(keywords)
    
    return kp_inst
    
    

def data_processing(df, kp_inst, fields):
    
    keys = [x for x in  kp_inst.keys()]
    
    assert len(fields) == len(keys)
    
    for i in range(len(fields)):
        for colname in fields[i]:
            df[colname + '_'  + keys[i]] = df[colname].apply(lambda x : len(kp_inst[keys[i]].extract_keywords(x)))
            
    return df

def scoring(df, score, query_string, class_name):
    
    def scoring_(x, score):
        if x==True:
            return score
        else:
            return 0
    
    df[class_name] = df.eval(query_string)   
    df[class_name] =df[class_name].apply(lambda x: scoring_(x, score)) 
    
    return df


# In[3]:


# import pymongo
# import json

# if __name__ == '__main__':
#     client = pymongo.MongoClient("mongodb://130.23.9.144:27017/", 27017, maxPoolSize=50)
#     d = dict((db, [collection for collection in client[db].collection_names()])
#              for db in client.database_names())
#     print(json.dumps(d))
    
# db = client.sources_1
# col =db.uspto
# # x =col.find()
# from bson.json_util import dumps
# # data = dumps(col.find_one())
# from bson.json_util import loads
# json_str = dumps(col.find())
# record2 = loads(json_str)
# import pandas as pd
# datadfdf = pd.DataFrame(record2)


# In[4]:


datadfdf.shape


# In[6]:


db.list_collection_names()


# In[15]:


catname, fields, keywords =dict_input(db,'infos')
kp_inst =kw_processor(catname, keywords)


# In[16]:


af1=data_processing(datadfdf, kp_inst, fields)


# In[18]:


dummyrec = af1.head()


# In[24]:


dummyrec = scoring(dummyrec, '10', '(title_NRK1>0) | (Description_NRK1 >0)', 'score1')
dummyrec = scoring(dummyrec, '10', '(title_NRK2>0)', 'score2')


# In[122]:


dummyrec = scoring(dummyrec, 15, '(title_NRK1>0) | (Description_NRK1 >0)', 'NRK1')


# In[35]:


list(db['infos'].find())[0]


# In[32]:


dummyrec


# In[46]:


dataquery = list(db['infos'].find())[0]


# In[47]:


dataquery


# In[51]:





# In[53]:


for i in range(len(dataquery['scoring'])):
       print(dataquery['scoring'][i])


# In[76]:


query = []
weight = []
dic = {}
name = []

for i in range(len(dataquery['scoring'])):
    dic = dataquery['scoring'][i]
    query.append(dic['query'])
    weight.append(dic['weight'])
    name.append(dic['name'])
    
query_weight = dict(zip(query, weight))    


# In[84]:


for ii in query_weight:
    print(ii)
    print(query_weight[ii])


# In[131]:


count = 0
for ii in query_weight:
    count = count+1
    dummyrec = scoring(dummyrec, str(query_weight[ii]), str(ii), str('S'+str(count)))


# In[124]:


dummyrec


# In[119]:


type(str(query_weight[ii]))


# In[107]:


def scoring(df, score, query_string, class_name):
    
    def scoring_(x, score):
        if x==True:
            return score
        else:
            return 0
    print('run')
    df[class_name] = df.eval(query_string)   
    df[class_name] =df[class_name].apply(lambda x: scoring_(x, score)) 
    
    return df


# In[106]:


dummyrec = scoring(dummyrec, query_weight[ii], iii, 'count')


# In[132]:


dummyrec


# In[ ]:


gapminder_ocean.drop(['1', 'gdpPercap', 'continent'], axis=1)

