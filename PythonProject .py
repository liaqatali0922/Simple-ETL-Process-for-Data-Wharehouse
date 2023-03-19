#!/usr/bin/env python
# coding: utf-8

# In[3]:


pip  install pandas


# In[4]:


import psycopg2 
import pandas as pd


# In[39]:


CA=pd.read_csv('Wealth-AccountsCountry.csv')


# In[40]:


CA.head()


# In[41]:


#cleaning data and take only desired coulmns
CA_clean=CA[['Code','Short Name','Table Name','Long Name','Currency Unit']]


# In[42]:


CA_clean.head()


# In[65]:


AD=pd.read_csv('Wealth-AccountData.csv')


# In[24]:


AD.head()


# In[70]:


#cleaning data and take only desired coulmns
AD_clean=AD[['Country Name','Country Code','Series Name','Series Code',
             '1995 [YR1995]','2000 [YR2000]','2005 [YR2005]','2010 [YR2010]',
             '2014 [YR2014]']]


# In[25]:


AD.columns


# In[72]:


AD_clean.columns


# In[81]:


AS=pd.read_csv('Wealth-AccountSeries.csv')


# In[6]:


AS.head()


# In[82]:


#cleaning data and take only desired coulmns
AS_clean=AS[['Code','Topic','Indicator Name','Long definition']]


# In[11]:


AS_clean.head()


# In[7]:


#function that is called in the later code for connection and execusion
def db_create():
    conn=psycopg2.connect("host=localhost dbname=postgres user=postgres password=Aa03029610374Zz")
    conn.set_session(autocommit=True)
    cur=conn.cursor()
    
    cur.execute("drop database if exists accounts")
    cur.execute("create database accounts")
    
    conn.close()
    conn=psycopg2.connect("host=localhost dbname=accounts user=postgres password=Aa03029610374Zz")
    cur=conn.cursor()
    
    return cur,conn
    
   


# In[21]:


cur,conn=db_create()


# In[55]:


cur.execute("drop table accountscountry")


# In[61]:


#creating table accountscountry and storing in AC_table_create
AC_table_create=("""create table accountscountry(
                country_code varchar,
                short_name varchar,
                table_name varchar,
                long_name varchar,
                currency_unit varchar)""")


# In[62]:


#execute the AC_table_create and incase of error rollback
try:
    cur.execute(AC_table_create)
    print("success")
except:
    cur.execute("rollback")
    cur.execute(AC_table_create)


# In[76]:


cur.execute("drop table accountsdata")


# In[77]:


#creating table accountsdata and storing in AD_table_create
AD_table_create=("""create table if not exists accountsdata(
                country_name varchar,
                country_code varchar,
                indicator_name varchar,
                indicator_code varchar,
                year_1995 varchar,
                year_2000 varchar,
                year_2005 varchar,
                year_2010 varchar,
                year_2014 varchar
                
            )""")

#execute the AD_table_create and incase of error rollback
try:
    cur.execute(AD_table_create)
    print("success")
except:
    cur.execute("rollback")
    cur.execute(AC_table_create)


# In[33]:


#creating table accountseries and storing in AS_table_create
AS_table_create=("""create table accountseries(
                series_code varchar,
                topic varchar,
                indicator_name varchar,
                long_definition varchar
                )""")

#execute the AS_table_create and incase of error rollback
try:
    cur.execute(AS_table_create)
    print("success")
except:
    cur.execute("rollback")
    cur.execute(AC_table_create)


# In[57]:


#inserting data in accountscountry table
CA_data_insert_table=("""insert into accountscountry values(%s,%s,%s,%s,%s)

""")


# In[63]:


for i,row in CA_clean.iterrows():
    cur.execute(CA_data_insert_table,list(row))


# In[89]:


conn.commit()


# In[68]:


#inserting data in accountsdata table
AD_data_insert_table=("""insert into accountsdata(
country_name,
country_code,
indicator_name,
indicator_code,
year_1995,
year_2000,
year_2005,
year_2010,
year_2014)
values(%s,%s,%s,%s,%s,%s,%s,%s,%s)
""")


# In[78]:


for i,row in AD_clean.iterrows():
    cur.execute(AD_data_insert_table,list(row))


# In[88]:


conn.commit()


# In[84]:


#inserting data in accountsdata table
AS_insert_data_table=("""insert into accountseries values(%s,%s,%s,%s)
""")


# In[86]:


for i,row in AS_clean.iterrows():
    cur.execute(AS_insert_data_table,list(row))


# In[87]:


conn.commit()


# In[ ]:


# Congratulation successfully created database in the postgreSQL using python jupytor notebook and created data model
# and populated all the tables 

