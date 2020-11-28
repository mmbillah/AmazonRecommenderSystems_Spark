#!/usr/bin/env python
# coding: utf-8

# In[1]:


# import libraries
from pyspark import SparkContext
from pyspark.ml.recommendation import ALS
from pyspark.ml.feature import StringIndexer
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
appName="Collaborative Filtering with PySpark"
# initialize the spark session
spark = SparkSession.builder.appName(appName).getOrCreate()
# get sparkcontext from the sparksession
sc = spark.sparkContext


# In[4]:


#define schema
schema = StructType([
    StructField("item", StringType(), True),
    StructField("user", StringType(), True),
    StructField("rating", StringType(), True),
    StructField("timestamp", IntegerType(), True)])
#read the file as a dataframe
df = spark.read.csv("Gift_Cards.csv",header=False,schema=schema)
#print the schema
df.printSchema()
#show the dataframe header
#df.show(n=5)
#number of rows
df.count()
#convert rating colum from string to integer
df = df.withColumn("rating", df["rating"].cast(IntegerType()))
df.show(n=5)
#provide index values for item and user to convert them into integers
stringIndexer = StringIndexer(inputCols=["item","user"], outputCols=["itemIndex","userIndex"])
model = stringIndexer.fit(df)
df_indexed = model.transform(df)
df_indexed.show(n=5)


# In[5]:


#split the data into training and testing set
(training, test) = df_indexed.randomSplit([0.8, 0.2])
#training the model
#define the model parameters
als = ALS(maxIter=5, 
          implicitPrefs=False,
          userCol="userIndex", 
          itemCol="itemIndex", 
          ratingCol="rating",
          coldStartStrategy="drop")
#train the model
model = als.fit(training)


# In[6]:


# predict using the testing datatset
predictions = model.transform(test)
predictions.show()


# In[8]:


def topLikes(dataframe,userIndex,limit):
 df = dataframe.filter(dataframe.userIndex==userIndex) .sort(dataframe.rating.desc()) .select(dataframe.userIndex,dataframe.itemIndex,dataframe.rating) .limit(limit)
 return df
# display top liked items for a user
topLikes(df_indexed,9386,10).show(truncate=False)


# In[13]:


def recommendedItems(userIndex,limit):
    test =  model.recommendForAllUsers(3)        
    		.filter(col('userIndex')==userIndex)        
    		.select(["recommendations.itemIndex","recommendations.rating"])        
    		.collect()
    return test
# display top 10 recommended artists for user 2062243
recommendedItems(9386,10)

