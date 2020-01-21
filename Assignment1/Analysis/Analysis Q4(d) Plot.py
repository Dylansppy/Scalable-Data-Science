#!/usr/bin/env python
# coding: utf-8

# # DATA420-19S1 Assignment 1 Peng Shen(57408055)
# # Analysis Q4(d)

# Plot the time series of TMIN and TMAX on the same axis for each station in New Zealand

# In[1]:


import numpy as np
import pandas as pd
import matplotlib.pyplot as plt 
import matplotlib.dates as mdates
import os
from collections import OrderedDict


# In[2]:


# Load the parquet file as pandas dataframe
df = pd.read_parquet('./daily_temp_NZ.parquet', engine='pyarrow')


# In[3]:


df.head()


# In[4]:


df.DATE = pd.to_datetime(df.DATE, format='%Y-%m-%d')
df.VALUE = df.VALUE/10


# In[5]:


df.head()


# In[6]:


df.info()


# In[7]:


df.VALUE.describe()


# In[8]:


df.DATE.describe()


# In[9]:


# Create a set of station IDs
stations = set(df.ID.tolist())
stations


# In[10]:


# Plot the time series of TMIN and TMAX on the same axis for each station in New Zealand
f, axes = plt.subplots(8, 2, dpi=300, figsize=(30, 20))  # affects output resolution (dpi) and font scaling (figsize)
f.subplots_adjust(hspace=0.6,wspace=0.1)
axes = axes.flatten()  # squeeze grid of (8, 2) axes into a linear array for iterating

for i, station in enumerate(stations):  # generate one plot for each tag
    print(station)

    # Load
    data = df[df.ID==station][["DATE", "ELEMENT", "VALUE"]]
    data = data.pivot_table(index='DATE',columns='ELEMENT',values='VALUE')

    # Plots
    a = axes[i]
    a.plot(data)  # assign label to include in legend
    a.set_ylim([-20, 40])  # exapnd axes slightly beyond [0, 10]
    a.set_xlim("1940", "2020")

    # Legend
    a.legend(data.columns,loc="best")

    # Labels
    a.set_title(f"Temperature time series for {station}")
    a.set_xlabel("Time")
    a.set_ylabel("Temperature ($^\circ$C)")
    
# Remove empty subplots
for a in axes[(i + 1):]:
    f.delaxes(a)


# In[11]:


# Outputs
output_path = os.path.expanduser("~/Documents/plots")  # M:/plots on windows
if not os.path.exists(output_path):
    os.makedirs(output_path)
    
# Save
plt.tight_layout()  # reduce whitespace
f.savefig(os.path.join(output_path, f"Time series of TMIN and TMAX for NZ stations.png"), bbox_inches="tight")  # save as png and view in windows
plt.close(f)


# In[12]:


# Plot the time series of TMIN and TMAX on the same axis for each station in New Zealand (resampling)
f, axes = plt.subplots(8, 2, dpi=300, figsize=(30, 20))  # affects output resolution (dpi) and font scaling (figsize)
f.subplots_adjust(hspace=0.6,wspace=0.1)
axes = axes.flatten()  # squeeze grid of (8, 2) axes into a linear array for iterating

for i, station in enumerate(stations):  # generate one plot for each tag
    print(station)

    # Load

    data = df[df.ID==station][["DATE", "ELEMENT", "VALUE"]]
    data = data.pivot_table(index='DATE',columns='ELEMENT',values='VALUE')
    data = data.resample('Y').mean()
    
    # Plots

    a = axes[i]
    a.plot(data)  # assign label to include in legend
    a.set_ylim([-20, 40])  # exapnd axes slightly beyond [0, 10]
    a.set_xlim("1940", "2020")

    # Legend
    a.legend(data.columns,loc="best")

    # Labels
    a.set_title(f"Temperature time series for {station}")
    a.set_xlabel("Time")
    a.set_ylabel("Temperature ($^\circ$C)")
    
# Remove empty subplots
for a in axes[(i + 1):]:
    f.delaxes(a)


# In[13]:


# Outputs
output_path = os.path.expanduser("~/Documents/plots")  # M:/plots on windows
if not os.path.exists(output_path):
    os.makedirs(output_path)
    
# Save
plt.tight_layout()  # reduce whitespace
f.savefig(os.path.join(output_path, f"Time series of TMIN and TMAX for NZ stations(2).png"), bbox_inches="tight")  # save as png and view in windows
plt.close(f)


# ## Plot the average time series for the entire country

# In[14]:


data = (
    df[["DATE", "ELEMENT", "VALUE"]]
    .groupby(['ELEMENT','DATE'])
    .agg({"VALUE": ['mean']})
    .rename(columns={'mean': 'AVG_TEMP'})
)
data.columns = data.columns.droplevel(0)
data.head()


# In[15]:


data = data.pivot_table(index='DATE',columns='ELEMENT',values='AVG_TEMP')


# In[16]:


data.describe()


# In[17]:


# Plot average time series of TMIN and TMAX for entire New Zealand
f, a = plt.subplots(dpi=300, figsize=(10, 5))  # affects output resolution (dpi) and font scaling (figsize)
a.plot(data, label=data.columns)  # assign label to include in legend
a.set_ylim([-5, 30])  # exapnd axes slightly beyond [0, 10]
a.set_xlim("1940", "2020")

# Legend
a.legend(data.columns)

# Labels
a.set_title(f"The average time series of TMIN and TMAX for New Zealand")
a.set_xlabel("Time")
a.set_ylabel("Temperature ($^\circ$C)")


# In[18]:


# Outputs
output_path = os.path.expanduser("~/Documents/plots")  # M:/plots on windows
if not os.path.exists(output_path):
    os.makedirs(output_path)
    
# Save
plt.tight_layout()  # reduce whitespace
f.savefig(os.path.join(output_path, f"Time series of TMIN and TMAX for entire New Zealand.png"), bbox_inches="tight")  # save as png and view in windows
plt.close(f)


# In[19]:


# Plot the average time series of TMIN and TMAX for entire New Zealand with resampling
f, a = plt.subplots(dpi=300, figsize=(10, 5))  # affects output resolution (dpi) and font scaling (figsize)
data = data.resample('Y').mean()
a.plot(data, label=data.columns)  # assign label to include in legend
a.set_ylim([-5, 30])  # exapnd axes slightly beyond [0, 10]
a.set_xlim("1940", "2020")

# Legend
a.legend(data.columns)

# Labels
a.set_title(f"The average time series of TMIN and TMAX for New Zealand")
a.set_xlabel("Time")
a.set_ylabel("Temperature ($^\circ$C)")


# In[20]:


# Outputs
output_path = os.path.expanduser("~/Documents/plots")  # M:/plots on windows
if not os.path.exists(output_path):
    os.makedirs(output_path)
    
# Save
plt.tight_layout()  # reduce whitespace
f.savefig(os.path.join(output_path, f"The average time series of TMIN and TMAX for entire New Zealand (2).png"), bbox_inches="tight")  # save as png and view in windows
plt.close(f)


# In[ ]:




