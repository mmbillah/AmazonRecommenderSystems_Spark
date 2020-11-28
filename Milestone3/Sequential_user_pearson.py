#!/usr/bin/env python
# coding: utf-8

# In this notebook, we will construct a sequential algorithm that recommends items using user-based collaborative filtering. We have been playing around with tweaking this as this approach is not perfect.

# In Future iterations I will probably put in a distance metric instead of using pearon corr (as you run into multiple problems just using pearson r.

# In[1]:


import pandas as pd
import gzip
import json
from math import sqrt
from scipy.stats import pearsonr
import warnings
warnings.filterwarnings('ignore')


# In[2]:


#load in data
def parse(path):
    g = gzip.open(path, 'rb')
    for l in g:
        yield json.loads(l)

def getDF(path):
    i = 0
    df = {}
    for d in parse(path):
        df[i] = d
        i += 1
    return pd.DataFrame.from_dict(df, orient='index')


path = r'G:\AWS_big_data\data\Software.json.gz'
df = getDF(path)

#Only keep 3 columns
df = df[['overall', 'asin', 'reviewerID']].sort_values('reviewerID')


# In[ ]:


#use a user's ratings (below is an example)
userInput = [
            {'asin':'B00UB76290', 'overall':5},
            {'asin':'B00CTTEKJW', 'overall':3.5},
            {'asin':'B00NG7JVSQ', 'overall':2},
            {'asin':'B00MCLGAZ4', 'overall': 4},
            {'asin':'B00H9A60O4', 'overall': 4},
            {'asin':'B00U1UELFE', 'overall': 1},
            {'asin':'B00D9ILKGS', 'overall': 4.5},
            {'asin':'B019QVZDSC', 'overall': 2},
         ]

inputItems = pd.DataFrame(userInput)
inputItems


# In[ ]:


#Filtering out users that have watched movies that the input has watched and storing it
userSubset = df[df['asin'].isin(inputItems['asin'].tolist())]
userSubset.head()


# In[ ]:


#Group ratings dataset by user
grouped = userSubset.groupby('reviewerID')
grouped = sorted(grouped,  key=lambda x: len(x[1]), reverse=True)
#only keep first 1000 as this is sequential algo
grouped = grouped[0:1000]


# In[ ]:


#get pearson corr when comparing user's ratings with out input user
pearsonCorrelationList = []
inputItems = inputItems.sort_values(by='asin')

#group contains items a user has rated
for user, group in grouped:
    group = group.sort_values(by='asin')
    nRatings = len(group)

    temp_df = inputItems[inputItems['asin'].isin(group['asin'].tolist())]
    nSimilarItems = len(temp_df)
    tempRatingList = temp_df['overall'].tolist()
    tempGroupList = group['overall'].tolist()
    
    if nRatings > 1:
        try:
            #r = calc_corr(tempRatingList, tempGroupList, nRatings)
            r = pearsonr(tempRatingList, tempGroupList)[0]
            pearsonCorrelationList.append([user, r, nSimilarItems])
        except:
            pearsonCorrelationList.append([user, 0, -1])

pearson_df = pd.DataFrame(pearsonCorrelationList, columns=['reviewerID', 'similarityIndex', 'nSimilarItems'])


# In[ ]:


#get top users (closest to our users
top_users=pearson_df.loc[(pearson_df.nSimilarItems>=3) & (pearson_df.similarityIndex.notna())].sort_values(by='similarityIndex', ascending=False)[0:50]
top_users.head()


# In[ ]:


#get other items that top users have rated
topUsersRating=top_users.merge(df, on='reviewerID', how='inner')
#create weightedRating
topUsersRating['weightedRating'] = topUsersRating['similarityIndex']*topUsersRating['overall']
recommendation_df = topUsersRating.groupby('asin').sum()[['similarityIndex','weightedRating']]
recommendation_df.columns = ['sum_similarityIndex','sum_weightedRating']
recommendation_df['weighted average recommendation score'] = recommendation_df['sum_weightedRating']/recommendation_df['sum_similarityIndex']
recommendation_df = recommendation_df.sort_values(by=['weighted average recommendation score', 'sum_similarityIndex'], ascending=[False, False])
recommendation_df.head()

