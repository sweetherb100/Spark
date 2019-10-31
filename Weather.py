
# coding: utf-8

# In[1]:


from collections import namedtuple

city = namedtuple("city", "id name")
city_records = namedtuple("city_records", "id_city date record_min_temp record_max_temp")
temp_data= namedtuple("temp_data","id_city date min_temp avg_temp max_temp")
other_data= namedtuple("other_data","id_city date wind_speed  min_humidity max_humidity pressure precipitation")


milan= city(1,"Milan")
rome= city(2,"Rome")

milan_records_1=city_records(1,"23/6",13,35)
milan_records_2=city_records(1,"24/6",10,35)
rome_records_1=city_records(2,"23/6",10,34)
rome_records_2=city_records(2,"24/6",13,35)

temp_data_1=temp_data(1,"23/6",23,28,33)
temp_data_2=temp_data(1,"24/6",23,28,34)
temp_data_3=temp_data(2,"23/6",20,28,33)
temp_data_4=temp_data(2,"24/6",24,29,35)

other_data_1=other_data(1,"23/6",11,35,73,1014.92,2.1)
other_data_2=other_data(1,"24/6",7,35,73,1012.84,2.2)
other_data_3=other_data(2,"23/6",15,20,40,1015.42,0)
other_data_4=other_data(2,"24/6",7,28,61,1013.84,2.2)

cityRDD=sc.parallelize([milan,rome])
city_recordsRDD=sc.parallelize([milan_records_1,milan_records_2,rome_records_1,rome_records_2])
temp_dataRDD=sc.parallelize([temp_data_1,temp_data_2,temp_data_3,temp_data_4])
other_dataRDD=sc.parallelize([other_data_1,other_data_2,other_data_3,other_data_4])




# In[2]:


#Select the city (providing its name), if any, that experienced the maximum temperature record

#determine per city daily record temp
city_max_temp_record=city_recordsRDD.map(lambda x: ((x[0],x[1]),x[-1]))
city_max_temp_record.collect()




# In[3]:


#determine per city daily max temp

max_tempRDD=temp_dataRDD.map(lambda x: ((x[0],x[1]),x[-1]))
max_tempRDD.collect()




# In[4]:


#select target cities
recordsRDD=max_tempRDD.join(city_max_temp_record).filter(lambda x: x[1][0]==x[1][1])
records_city_ids=recordsRDD.map(lambda x: x[0][0]).collect()
print records_city_ids



# In[5]:



#determine city name
cityRDD.filter(lambda x: x[0] in records_city_ids).map(lambda x: x[1]).collect()


# In[6]:


cityDF = cityRDD.toDF(["id", "name"])
city_recordsDF = city_recordsRDD.toDF(["id_city", "date", "record_min_temp", "record_max_temp"])
temp_dataDF = temp_dataRDD.toDF(["id_city", "date", "min_temp", "avg_temp", "max_temp"])
other_dataDF = other_dataRDD.toDF(["id_city", "date", "wind_speed",  "min_humidity", "max_humidity", "pressure", "precipitation"])


# In[8]:


#determine per city daily record temp
city_max_temp_record_DF=city_recordsDF.select("id_city","date","record_max_temp")
city_max_temp_record_DF.show()


# In[11]:


#determine per city daily max temp

max_temp_DF=temp_dataDF.select("id_city","date","max_temp")
max_temp_DF.show()



# In[24]:


#select target cities
recordsDF=max_temp_DF.join(city_max_temp_record_DF,(city_max_temp_record_DF.id_city==max_temp_DF.id_city)                          &(city_max_temp_record_DF.date==max_temp_DF.date)                          &(city_max_temp_record_DF.record_max_temp==max_temp_DF.max_temp)).select(city_max_temp_record_DF.id_city)



# In[40]:


#determine city name
records_city_DF = cityDF.join(recordsDF,recordsDF.id_city==cityDF.id).select("name")

records_city_DF.show()

