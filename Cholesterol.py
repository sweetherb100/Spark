
# coding: utf-8

# In[10]:

import collections
from operator import add


# In[11]:

Patient = collections.namedtuple("Patient", "id name address phone_number")

BloodTest = collections.namedtuple("BloodTest", "id id_patient code date description result")


# In[12]:

patient1=Patient(1, "Alice", "Pza L. Da Vinci 32", "02 2399")
patient2=Patient(2, "Bob", "Via Golgi 40", "02 23992")
bloodTest1=BloodTest(1,1,1, "17/2/2017", "Glucose",94)
bloodTest2=BloodTest(2,1,2, "17/2/2017", "Cholesterol",140)
bloodTest3=BloodTest(3,2,1, "16/2/2017", "Glucose",83)
bloodTest4=BloodTest(4,2,2, "16/2/2017", "Cholesterol",238)


# In[13]:

patientsRDD=sc.parallelize([patient1,patient2])
testsRDD=sc.parallelize([bloodTest1,bloodTest2,bloodTest3,bloodTest4])


# In[15]:

filteredRDD=testsRDD.filter(lambda x : x[4]=="Cholesterol" and x[5]>220)


# In[16]:

filteredRDD.collect()


# In[20]:

mappedRDD=filteredRDD.map(lambda x : (x[1], x[0]))
mappedRDD.collect()


# In[21]:

joinedRDD=mappedRDD.join(patientsRDD)
joinedRDD.collect()


# In[23]:

resultsRDD=joinedRDD.map(lambda x : x[1][1])
resultsRDD.collect()


# In[ ]:



