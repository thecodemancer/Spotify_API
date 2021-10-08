#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np
import sqlite3
sqlite3.register_adapter(np.int64, lambda val: int(val))

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import create_engine
from pprint import pprint as pp
import pandas as pd
import random


# In[14]:


class SQliteAdapter:
    def __init__(self):
        self.my_conn=None
        self.connect()
        self.create_table_my_played_tracks()        
        
    def connect(self):
        try:
            self.engine  = create_engine("sqlite:///my_played_tracks.sqlite")   
            self.my_conn = self.engine.raw_connection()

        except SQLAlchemyError as e:
            error = str(e.__dict__['orig'])
            pp(error)    
        else:
            pp("engine created successfully...")   

    def close(self):
        try:
            self.my_conn.close()  
        except SQLAlchemyError as e:
            error = str(e.__dict__['orig'])
            pp(error)    
        else:
            pp("engine closed")          
    def drop_table(self, name):
        try:
            self.my_conn.execute('''
                DROP TABLE ''' + name
                )  
        except SQLAlchemyError as e:
            error = str(e.__dict__['orig'])
            pp(error)
        else:
            pp(name + " table droped successfully...")    
         
    def create_table_my_played_tracks(self):
        try:
            self.my_conn.execute('''
        CREATE TABLE IF NOT EXISTS my_played_tracks(
            song_name VARCHAR(200),
            artist_name VARCHAR(200),
            played_at VARCHAR(200),
            timestamp VARCHAR(200),
            CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
        )
                                      ''')  
        except SQLAlchemyError as e:
            error = str(e.__dict__['orig'])
            pp(error)
        else:
            pp("my_played_tracks table created successfully...") 
            
    def insert_into_my_played_tracks(self, df):
        try:
            cursor = self.my_conn.cursor()

            my_query="INSERT INTO my_played_tracks (song_name, artist_name, played_at, timestamp) VALUES(?,?,?,?);"

            cursor.executemany(my_query, list(df.to_records(index=False)) ) 

            self.my_conn.commit()
            cursor.close()

        except SQLAlchemyError as e:
            cursor.close()
            error = str(e.__dict__['orig'])
            pp(error)
        else:
            pp("insert_into_repos executed successfully...")     
        finally:
            cursor.close()

            ##Create table repos_libraries
    def execute_my_query(self, sql):
        try:
            r_set=a.my_conn.execute(sql)  
            for row in r_set.fetchall():
                pp(row)

        except SQLAlchemyError as e:
            #print(e)
            error = str(e.__dict__['orig'])
            pp(error)
        else:
            pp("my_played_tracks query done!")


# # Testing

# In[15]:


def generate_random_data():
    random_number=str(random.randint(0,1000000))
    data=[
            {
                'song_name':'texto 1',
                'artist_name':'texto 2',
                'played_at':'texto '+random_number,
                'timestamp':'texto 4'
             }
         ]
    return data


# In[16]:


data = generate_random_data()
df=pd.DataFrame.from_dict(data)
df


# In[17]:


a=SQliteAdapter()
a.connect()
a.insert_into_my_played_tracks( df )
a.close()


# In[20]:


a.connect()
a.execute_my_query('''
    SELECT * FROM my_played_tracks ORDER BY played_at ;
''')
a.close()


# In[ ]:





# In[ ]:




