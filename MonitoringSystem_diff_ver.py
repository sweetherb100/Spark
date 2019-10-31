
# coding: utf-8

# In[8]:


import collections
from operator import add


# In[9]:


System = collections.namedtuple("System", "id location repair_cost downtime_cost")

EventSystem1 = collections.namedtuple("EventSystem1", "id day month year event_type event_value reference_system_id")
# as EventSystem1 but date is a single string DD:MM:YYYY
EventSystem2 = collections.namedtuple("EventSystem2", "id date event_type event_value reference_system_id")


# In[10]:


event1=EventSystem1(1,31, 8, 2016, "Entrance alarm", 0.0, 1)
event2=EventSystem1(2,31, 8, 2016, "Window alarm", 0.0, 1)
event3=EventSystem1(3,31, 8, 2016, "Movement detection", 0.0, 1) 
event4=EventSystem2(4,"31:8:2016", "Temperature sample", 28.0, 2) 
event5=EventSystem2(5,"31:8:2016", "Temperature sample", 27.2, 2) 
event6=EventSystem2(6,"31:8:2016", "Temperature sample", 27.3, 1) 
event7=EventSystem1(3,31, 8, 2016, "Fire alarm", 0.0, 1)
EventsSys1RDD=sc.parallelize([event1,event2,event3,event7]) 
EventsSys2RDD=sc.parallelize([event4,event5,event6])
System1=System(1,"GroundFloor",100.0, 1.3) 
System2=System(2,"BedRoom",11.0, 0.0)
SystemsRDD=sc.parallelize([System1,System2])


# In[11]:


# Convert EventSystem1 to EventSystem2 format  
def eventsConversion(e1):
    return EventSystem2(e1.id,str(e1.day)+":"+str(e1.month)+":"+str(e1.year), e1.event_type, e1.event_value, e1.reference_system_id)



# In[12]:


# Compute the union of events by adopting as a reference EventSystem2 format
unionEventsRDD = EventsSys2RDD.union(EventsSys1RDD.map(eventsConversion)) 
print unionEventsRDD.collect()
# [EventSystem2(id=4, date='31:8:2016', event_type='Temperature sample', event_value=28.0, reference_system_id=2), 
#  EventSystem2(id=5, date='31:8:2016', event_type='Temperature sample', event_value=27.2, reference_system_id=2), 
#  EventSystem2(id=6, date='31:8:2016', event_type='Temperature sample', event_value=27.3, reference_system_id=2), 
#  EventSystem2(id=1, date='31:8:2016', event_type='Entrance alarm', event_value=0.0, reference_system_id=1), 
#  EventSystem2(id=2, date='31:8:2016', event_type='Window alarm', event_value=0.0, reference_system_id=1), 
#  EventSystem2(id=3, date='31:8:2016', event_type='Movement detection', event_value=0.0, reference_system_id=1), 
#  EventSystem2(id=3, date='31:8:2016', event_type='Fire alarm', event_value=0.0, reference_system_id=1)]


# In[13]:


# Select the Temperature samples
filteredEventsRDD=unionEventsRDD.filter(lambda e: e.event_type == "Temperature sample")
print filteredEventsRDD.collect()
# [EventSystem2(id=4, date='31:8:2016', event_type='Temperature sample', event_value=28.0, reference_system_id=2), 
#  EventSystem2(id=5, date='31:8:2016', event_type='Temperature sample', event_value=27.2, reference_system_id=2), 
#  EventSystem2(id=6, date='31:8:2016', event_type='Temperature sample', event_value=27.3, reference_system_id=1)]


# In[16]:


# Compute the number of temperature samples per system

#First convert raw events in preparation of the join operation
mappedEventsRDD=filteredEventsRDD.map(lambda e: (e.reference_system_id, (e.id, e.date, e.event_type, e.event_value)))
mappedEventsRDD.collect()
# [(2, (4, '31:8:2016', 'Temperature sample', 28.0)), 
#  (2, (5, '31:8:2016', 'Temperature sample', 27.2)), 
#  (1, (6, '31:8:2016', 'Temperature sample', 27.3))]



# In[17]:


#Convert system  in preparation of the join operation
mappedSystemsRDD= SystemsRDD.map(lambda s: (s.id, (s.location, s.repair_cost, s.downtime_cost)))
mappedSystemsRDD.collect()
# [(1, ('GroundFloor', 100.0, 1.3)), 
#  (2, ('BedRoom', 11.0, 0.0))]


# In[18]:


joinedEventsRDD=mappedEventsRDD.join(mappedSystemsRDD) 
print joinedEventsRDD.collect() 
# [(1, ((6, '31:8:2016', 'Temperature sample', 27.3), ('GroundFloor', 100.0, 1.3))), 
#  (2, ((5, '31:8:2016', 'Temperature sample', 27.2), ('BedRoom', 11.0, 0.0))), 
#  (2, ((4, '31:8:2016', 'Temperature sample', 28.0), ('BedRoom', 11.0, 0.0)))]


# In[19]:


# Compute events number 
systemsTemperatureEventsRDD=joinedEventsRDD.map(lambda e: (e[0],1)).reduceByKey(add)
print systemsTemperatureEventsRDD.collect()

