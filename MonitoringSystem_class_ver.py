
# coding: utf-8

# In[1]:


import collections
from operator import add


# In[2]:


System = collections.namedtuple("System", "id location repair_cost downtime_cost")

EventSystem1 = collections.namedtuple("EventSystem1", "id day month year event_type event_value reference_system_id")
# as EventSystem1 but date is a single string DD:MM:YYYY
EventSystem2 = collections.namedtuple("EventSystem2", "id date event_type event_value reference_system_id")


# In[3]:


event1=EventSystem1(1,31, 8, 2016, "Entrance alarm", 0.0, 1)
event2=EventSystem1(2,31, 8, 2016, "Window alarm", 0.0, 1)
event3=EventSystem1(3,31, 8, 2016, "Movement detection", 0.0, 1) 
event4=EventSystem2(4,"31:8:2016", "Temperature sample", 28.0, 2) 
event5=EventSystem2(5,"31:8:2016", "Temperature sample", 27.2, 2) 
event6=EventSystem2(6,"31:8:2016", "Temperature sample", 27.3, 2) 
event7=EventSystem1(3,31, 8, 2016, "Fire alarm", 0.0, 1)
EventsSys1RDD=sc.parallelize([event1,event2,event3,event7]) 
EventsSys2RDD=sc.parallelize([event4,event5,event6])
System1=System(1,"GroundFloor",100.0, 1.3) 
System2=System(2,"BedRoom",11.0, 0.0)
SystemsRDD=sc.parallelize([System1,System2])


# In[4]:


# Convert EventSystem1 to EventSystem2 format  
def eventsConversion(e1):
    return EventSystem2(e1.id,str(e1.day)+":"+str(e1.month)+":"+str(e1.year), e1.event_type, e1.event_value, e1.reference_system_id)



# In[5]:


EventsSystem1_2RDD = EventsSys1RDD.map(lambda x : eventsConversion(x))


# In[6]:


EventsSystem1_2RDD.collect()


# In[7]:


EventsRDD = EventsSystem1_2RDD.union(EventsSys2RDD)


# In[8]:


EventsRDD.collect()


# In[9]:


tempsRDD = EventsRDD.filter(lambda x : x.event_type == "Temperature sample")


# In[10]:


tempsRDD.collect()


# In[11]:


resultsRDD = tempsRDD.map(lambda x : (x.reference_system_id, 1)).reduceByKey(lambda x,y : x+y)


# In[12]:


resultsRDD.collect()


# In[15]:


results_2RDD = SystemsRDD.map(lambda x : (x.id,x.location)).join(resultsRDD).map(lambda x : (x[0],x[1][0],x[1][1]))


# In[16]:


results_2RDD.collect()

