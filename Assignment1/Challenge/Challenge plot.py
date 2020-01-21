#!/usr/bin/env python
# coding: utf-8

# # DATA 420 Assignment 1 Peng Shen (57408055)
# # Challenge
# ## Q1

# In[1]:


import numpy as np
import pandas as pd
import matplotlib.pyplot as plt 
import plotly.plotly as py
import plotly.tools as tls
import os
from collections import OrderedDict


# ### Investigating other elements

# In[2]:


# Load the parquet file as pandas dataframe
df = pd.read_parquet('./element_count_by_day.parquet', engine='pyarrow')


# In[3]:


df.head()


# In[4]:


df.DATE = pd.to_datetime(df.DATE, format='%Y-%m-%d')
df.head()


# In[5]:


df.info()


# In[6]:


df.DATE.describe()


# In[7]:


df.ELEMENT_COUNT.describe()


# In[8]:


data = df.pivot_table(index='DATE',columns='ELEMENT',values='ELEMENT_COUNT')


# In[9]:


# Plot the time series of frequency of each ‘other’ element collected by stations all over the world
f, a = plt.subplots(dpi=300, figsize=(10, 5))  # affects output resolution (dpi) and font scaling (figsize)
data = data.resample('Y').mean()
a.plot(data, label=data.columns)  # assign label to include in legend
a.set_ylim([0, 6720])  # exapnd axes slightly beyond [1, 6720]
a.set_xlim("1851", "2020")

# Legend
a.legend(data.columns, title='element',fontsize=10, handlelength=5)

# Labels
a.set_title(f"The Time Series of Top10 Most Frequently Recorded ‘Other’ Elements")
a.set_xlabel("Year")
a.set_ylabel("Number of Stations with The Record")


# In[10]:


# Outputs
output_path = os.path.expanduser("~/Documents/plots")  # M:/plots on windows
if not os.path.exists(output_path):
    os.makedirs(output_path)
    
# Save
plt.tight_layout()  # reduce whitespace
f.savefig(os.path.join(output_path, f"The time series of top10 most frequently recorded ‘other’ elements.png"), bbox_inches="tight")  # save as png and view in windows
plt.close(f)


# ### Geographical map of average wind speed of each states in 2015

# In[11]:


# Load the parquet file as pandas dataframe
df = pd.read_parquet('./AWND_2015_by_states.parquet', engine='pyarrow')
df.AWND_2015 = df.AWND_2015/10
df.head()


# In[12]:


tls.set_credentials_file(username='dylansp', api_key='I3hOHdVaQKa1gbSLegzU')


# In[13]:


df.describe()


# In[14]:


# Define elements for 
data = [dict(type='choropleth', 
             autocolorscale=True, 
             locations=df.STATE, 
             z=df.AWND_2015, 
             locationmode='USA-states',
             colorbar=dict(title='Average Wind Speed(m/s)')
            )
       ]
data


# In[15]:


# Define layout
layout = dict(title='Average Wind Speed for USA States in 2015',
             geo = dict(scope='usa', 
                        projection=dict(type='albers usa')
                       )
             )
layout


# In[16]:


fig = dict(data=data, layout=layout)
py.iplot(fig, filename='Wind Speed')


# ### Time Series of Daily Average Wind Speed for Each States in 2015

# In[17]:


df.count()


# In[18]:


# Get 10 representative states
top_states = list(df.sort_values('AWND_2015', ascending=False).STATE[0:50:10])
top_states


# In[19]:


# Load the parquet file as pandas dataframe
df = pd.read_parquet('./daily_AWND_by_states.parquet', engine='pyarrow')
df = df[df.STATE.isin(top_states)]
df.AVG_STATE_WIND = df.AVG_STATE_WIND/10
df.head()


# In[20]:


df.DATE = pd.to_datetime(df.DATE, format='%Y-%m-%d')
df.head()


# In[21]:


df.info()


# In[22]:


df.AVG_STATE_WIND.describe()


# In[23]:


data = df.pivot_table(index='DATE',columns='STATE',values='AVG_STATE_WIND')


# In[24]:


# Plot the time series of frequency of each ‘other’ element collected by stations all over the world
f, a = plt.subplots(dpi=300, figsize=(10, 5))  # affects output resolution (dpi) and font scaling (figsize)
a.plot(data, label=data.columns)  # assign label to include in legend
a.set_ylim([0, 15])  # exapnd axes slightly beyond [1, 6720]
a.set_xlim("2015-01-01", "2016-01-01")

# Legend
a.legend(data.columns, title='State',fontsize=5, handlelength=5)

# Labels
a.set_title(f"Time Series of Daily Average Wind Speed for 5 Representative USA States in 2015")
a.set_xlabel("Days")
a.set_ylabel("Average Wind Speed(m/s)")


# In[25]:


# Outputs
output_path = os.path.expanduser("~/Documents/plots")  # M:/plots on windows
if not os.path.exists(output_path):
    os.makedirs(output_path)
    
# Save
plt.tight_layout()  # reduce whitespace
f.savefig(os.path.join(output_path, f"Time series of daily average wind speed for 5 selected USA states in 2015.png"), bbox_inches="tight")  # save as png and view in windows
plt.close(f)


# In[ ]:




