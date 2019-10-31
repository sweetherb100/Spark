
# coding: utf-8

# In[1]:


from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark import SparkContext
from operator import add


# In[2]:



import ibmos2spark



configuration_name = 'os_eb857c6286cd4a46aea53716d6c4bbd6_configs'
cos = ibmos2spark.CloudObjectStorage(sc, credentials, configuration_name, 'bluemix_cos')

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df_data_1 = spark.read  .format('org.apache.spark.sql.execution.datasources.csv.CSVFileFormat')  .option('header', 'true')  .load(cos.url('prices.csv', 'apccourse2017180c7db5912f304fcd8f4cf0d147b8b65c'))
df_data_1.take(5)


# In[3]:


dataRDD=df_data_1.rdd


# In[4]:


dataRDD.take(5)


# In[5]:


#Select the total volumes and the totals for Apple
totals=dataRDD.map(lambda x:int(x[-1])).reduce(lambda x,y:x+y)
print totals
appleRDD=dataRDD.filter(lambda x:x[0]=="AAPL")
appleRDD.cache()
print appleRDD.count()
appleRDD.map(lambda x:int(x[-1])).reduce(add)


# In[6]:


# Determine the session(s) with the highest volume exchange

max_vals=dataRDD.map(lambda x: x[-1]).max()

print max_vals

dataRDD.filter(lambda x: x[-1]==max_vals).collect()


# In[7]:


#Determine company, date and time of the session(s) with
#the highest variability (max delta between high and low)

def delta(x,y):
    return x-y

deltaRDD=dataRDD.map(lambda x: (x[0],x[1],x[2],delta(float(x[-4]),float(x[-3]))))
#deltaRDD=dataRDD.map(lambda x: (x[0:3],delta(float(x[-4]),float(x[-3]))))
deltaRDD.take(1)

max_var_sessions=deltaRDD.map(lambda x: x[-1]).max()

max_var_sessionsRDD=deltaRDD.filter(lambda x: x[-1] == max_var_sessions)


max_var_sessionsRDD.collect()
#########################################################################

# In[8]:


import time
import datetime

def time_conv(s):
    return time.mktime(datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S").timetuple())



# In[9]:


#Select all transactions on 2016-12-21 beween 15:00:00 and 16:00:00
timedRDD=dataRDD.filter(lambda row: time_conv("2016-12-21 15:00:00")  <=time_conv(row[1]+ " " + row[2])<=time_conv("2016-12-21 15:00:00"))
timedRDD.take(5)


# In[10]:


df_data_1.printSchema()
#Select the total volumes and the totals for Apple
from pyspark.sql.types import DoubleType
from pyspark.sql.types import IntegerType


df_data=df_data_1.withColumn("OPEN", df_data_1["OPEN"].cast("double")).withColumn("HIGH", df_data_1["HIGH"].cast("double")).withColumn("LOW", df_data_1["LOW"].cast("double")).withColumn("CLOSE", df_data_1["CLOSE"].cast("double")).withColumn("VOLUME ", df_data_1["VOLUME "].cast("int"))

df_data.printSchema()
totals=df_data.groupBy().sum("volume ").collect()[0][0]
print totals
df_apple=df_data.filter("SYMBOL == 'AAPL'")
df_apple.cache()
print df_apple.count()
df_apple.groupBy().sum("volume ").collect()[0][0]


# In[11]:


#Determine company, date and time of the session(s) with
#the highest variability (max delta between high and low)
from pyspark.sql.functions import udf

delta_udf=udf(delta,DoubleType())


df_delta=df_data.select("SYMBOL","DATE","TIME",delta_udf("HIGH","LOW")).withColumnRenamed("delta(HIGH, LOW)","DIFF_HIGH_LOW")
df_delta.show()

#max_var_sessions=df_delta.select("DIFF_HIGH_LOW").groupBy().max().collect()[0][0]

#print max_var_sessions


# In[12]:


df_max_var_sessions=df_delta.filter( max_var_sessions -0.01 <= df_delta.DIFF_HIGH_LOW)


df_max_var_sessions.show()
