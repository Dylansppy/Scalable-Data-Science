#!/usr/bin/env python
# coding: utf-8

# # DATA420-19S1 Assignment 1 Peng Shen(57408055)
# # Analysis Q4(e)

# plot the cumulative rainfall for each country

# In[12]:


import numpy as np
import pandas as pd
import plotly.plotly as plt
import plotly.tools as tls


# In[50]:


tls.set_credentials_file(username='dylansp', api_key='I3hOHdVaQKa1gbSLegzU')


# In[33]:


# Load the parquet file as pandas dataframe
df = pd.read_parquet('./cum_rainfall_by_country.parquet', engine='pyarrow')
df.head()


# In[34]:


df.describe()


# In[35]:


# load the dataset containing 3-letter country code with corresponding 2-letter code
country = pd.read_csv("country-codes.csv")
country.head()


# In[36]:


# Create a subset of country dataframe above
country = country.iloc[:, [0,1,2]]
country.columns = ['COUNTRY_NAME', 'ALPHA2_CODE','ALPHA3_CODE' ]
country.head()


# In[37]:


# Add alpha-3 code to our data
df = pd.merge(df, country, how='left', on=['COUNTRY_NAME'])
df.head()


# In[38]:


# We can find that some inconsistancy between two tables in country name, then we will join on Alpha-2 code
df = pd.merge(df, country, how='left', left_on='COUNTRY_CODE', right_on='ALPHA2_CODE' )
df.head()


# In[42]:


# Get the final table with Alpha-3 code
mask = df.ALPHA3_CODE_x.isnull()
df.loc[mask,'ALPHA3_CODE_x'] = df[mask]['ALPHA3_CODE_y']
df.head()


# In[52]:


# Define elements for 
data = [dict(type='choropleth', 
             autocolorscale=True, 
             locations=df.ALPHA3_CODE_x, 
             z=df.CUM_RAINFALL, 
             locationmode='ISO-3',
             colorbar=dict(title='cumulative rainfall')
            )
       ]
data


# In[53]:


# Define layout
layout = dict(title='Cumulative Rainfall for Countries',
             geo = dict(scope='world', 
                        projection=dict(type='natural earth')
                       )
             )
layout


# In[54]:


fig = dict(data=data, layout=layout)
plt.iplot(fig, filename='Cumulative Rainfall')


# In[ ]:




