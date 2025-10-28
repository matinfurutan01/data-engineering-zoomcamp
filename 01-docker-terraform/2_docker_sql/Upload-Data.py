#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd


# In[2]:


pd.__version__


# In[3]:


df = pd.read_csv('yellow_tripdata_2021-01.csv.gz', nrows=100)


# In[11]:


df['tpep_pickup_datetime'] = pd.to_datetime(df.tpep_pickup_datetime)
df['tpep_dropoff_datetime'] = pd.to_datetime(df.tpep_dropoff_datetime)


# In[12]:


df


# In[14]:


from sqlalchemy import create_engine


# In[16]:


engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')


# In[17]:


engine.connect()


# In[19]:


print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))


# In[20]:


df_iter = pd.read_csv('yellow_tripdata_2021-01.csv.gz', iterator=True, chunksize=100000)


# In[23]:


df = next(df_iter)


# In[25]:


len(df)


# In[29]:


df['tpep_pickup_datetime'] = pd.to_datetime(df.tpep_pickup_datetime)
df['tpep_dropoff_datetime'] = pd.to_datetime(df.tpep_dropoff_datetime)


# In[ ]:





# In[30]:


df.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')



from time import time


# In[37]:


while True:

    t_start = time.time()
    
    df = next(df_iter)

    df['tpep_pickup_datetime'] = pd.to_datetime(df.tpep_pickup_datetime)
    df['tpep_dropoff_datetime'] = pd.to_datetime(df.tpep_dropoff_datetime)

    df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')

    t_end = time.time()

    print('inserted another chunk..., took %.3f second' %(t_end - t_start))





