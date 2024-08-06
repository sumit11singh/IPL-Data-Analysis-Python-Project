#!/usr/bin/env python
# coding: utf-8

# In[6]:


import pandas as pd
import numpy as np
import pymysql
from pymysql import connect


# # connecting mysql with python

# In[10]:


connection=pymysql.connect(host='localhost',user='root',password='Password',database='ipl_database')


# In[11]:


query='select* from ipl_details'


# In[159]:


ipl=pd.read_sql(query,connection)


# In[160]:


ipl


# In[17]:


ipl.head(2)


# In[18]:


ipl.tail()


# In[19]:


ipl.sample(4)


# In[20]:


ipl.info()


# In[21]:


ipl.describe()


# conslusion:As per data no of matches played are 756
# ,avg win by runs:13
# ,largest win by runs:146
# ,avg win by wickets:3
# ,max win by wickets:10
# 

# # Total matches win by each team 

# In[168]:


ipl['winner'].value_counts().head(5).reset_index()


# In[167]:


ipl['winner'].value_counts().head(5).plot(kind='bar')


# conclusion:Top winner teams are listing as above

# # Total matches played citywise

# In[166]:


ipl[ipl['city']=='Hyderabad'].shape[0]


# In[32]:


def get_match_count(city):
    return ipl[ipl['city'] == city].shape[0]

    
    


# In[33]:


print(get_match_count('Visakhapatnam'))


# # Matched played citywise & after 2010

# In[136]:


m=ipl['city']=='Hyderabad'
m2=ipl['date']>'05-04-2010'
ipl[m&m2].shape[0]


# In[177]:


def get_match_count(city,date):
    mask1=ipl['city']=='city' 
    mask2=ipl['date']>'date'
    return ipl[mask1 & mask2].shape[0]

    
  


# In[179]:


#print(get_match_count('Hyderabad','08-05-2019'))


# In[140]:


ipl


# In[182]:


ipl['toss_decision'].value_counts().reset_index()


# In[158]:


(ipl['toss_decision'].value_counts()/ipl.shape[0])*100


# In[141]:


ipl['toss_decision'].value_counts().plot(kind='pie')


# Conclusion: as per above in 463 matched toss winning teams are choosing field & only 293 matches toss winning teams are choosing bat first so percentage ratio is approx 62% & 38%

# In[142]:


ipl


# # Total matches played by each team

# In[143]:


total_matches_played_by_each_team=ipl['team1'].value_counts()+ipl['team2'].value_counts()


# In[48]:


total_matches_played_by_each_team.sort_values(ascending=False)


# Conclusion: There is a list of total matches played by each team

# # Unique cities where matches were played 

# In[49]:


ipl.drop_duplicates(subset=['city']).shape[0]


# As per above results there are 33 unique cities where matches were played

# # IPL winner team in each Season

# In[186]:


ipl.drop_duplicates(subset='Season',keep='last')[['Season','winner']].sort_values('Season')


# Conclusion:thease are teams listed above season wise whose have won the IPL Season.

# # import delivery file through mysql into jypitor notebook

# In[263]:


query1='SELECT * FROM deliveries;'


# In[264]:


delivery=pd.read_sql(query1,connection)


# In[265]:


delivery


# # Top Batsman by runs

# In[266]:


top_5_batsman=delivery.groupby('batsman')
top_5_batsman.sum()['batsman_runs'].sort_values(ascending=False).head(5)


# above is list of top 5 batsmans

# # Total four Count which are hited by batsman

# In[267]:


mask1=delivery['batsman_runs']==4

new_delivery=delivery[mask1]


# In[268]:


new_delivery=new_delivery.groupby('batsman')


# In[270]:


new_delivery.size().sort_values(ascending=False).head(5)


# above is the list of top 5 batsmans who hited max fours in all season.

# # Total sixes Count which are hited by batsman

# In[209]:


mask1=delivery['batsman_runs']==6

new_delivery=delivery[mask1]

new_delivery=new_delivery.groupby('batsman')


# In[210]:


new_delivery.count()['batsman_runs'].sort_values(ascending=False).head(5)


# above is the list of top 5 batsmans who hited sixes in all season.

# # 2nd method

# In[211]:


mask1=delivery['batsman_runs']==6

new_delivery=delivery[mask1]

new_delivery=new_delivery.groupby('batsman')


# In[212]:


new_delivery.size().sort_values(ascending=False).head(5)


# # Max Runs Maker  against Bowling Team

# In[213]:


vk=delivery[delivery['batsman']=='DA Warner']


# In[271]:


vk.groupby('bowling_team').sum()['batsman_runs'].sort_values(ascending=False).head(3)


# In[274]:


def batsman_score(batsman_name):
    vk=delivery[delivery['batsman']==batsman_name]
    return vk.groupby('bowling_team').sum()['batsman_runs'].sort_values(ascending=False).head(5)


# In[275]:


batsman_score('DA Warner')


# conclusion: as per above these are the 5 bowling teams,against which DA warner has made the max runs.

# # Finding SR for Each batsman in Death_over

# In[217]:


death_over=delivery[delivery['over']>15]


# In[218]:


all_batsman=death_over.groupby('batsman').count()['batsman_runs'].sort_values(ascending=False)


# In[219]:


all_batsman


# In[220]:


x=all_batsman>200
batsman_list=all_batsman[x].index.to_list()


# In[221]:


batsman_list


# In[222]:


final=delivery[delivery['batsman'].isin(batsman_list)]


# In[223]:


runs=final.groupby('batsman').sum()['batsman_runs'].sort_values(ascending=False)


# In[224]:


runs


# In[225]:


balls=final.groupby('batsman').count()['batsman_runs']


# In[226]:


balls


# In[227]:


sr=(runs/balls)*100


# In[228]:


sr


# # coclusion:steps used are as below..
#     1-using masking or filtering the data 
#     2-use groupby function
#     3-boolian indexing
#     4-isin function
#     5-count function
#     6-sum function
#     
#     By using above codes we can calculate strike rate(SR) in death over from 16-20.

# # Merging IPL & Delivery table with the help of joins

# In[257]:


new=delivery.merge(ipl,left_on='match_id',right_on='id')


# In[258]:


new.shape


# In[232]:


new


# # Finding Orange Cup Winner

# In[276]:


new.groupby(['Season','batsman']).sum()['batsman_runs'].sort_values(ascending=False).reset_index().drop_duplicates(subset='Season',keep='first').sort_values('Season')


# Conclusion: These are above orange cap winner in each IPL Season.

# In[ ]:




