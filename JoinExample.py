
# coding: utf-8

# In[8]:


import collections
Rel_A=collections.namedtuple('Rel_A', 'k a1 a2')
Rel_B=collections.namedtuple('Rel_B', 'k b1 b2')


# In[30]:


rel_a1=Rel_A("k1",1,2)
rel_a2=Rel_A("k2",3,4)
rel_a3=Rel_A("k3",5,6)
rel_b1=Rel_B("k1",7,8)
rel_b2=Rel_B("k2",9,10)
rel_b3=Rel_B("k3",11,12)


# In[31]:


rel_a_rdd=sc.parallelize([rel_a1,rel_a2,rel_a3])
rel_b_rdd=sc.parallelize([rel_b1,rel_b2,rel_b3])


# In[32]:


rel_a_rdd.collect()


# In[33]:


rel_b_rdd.collect()


# In[34]:


rel_c_rdd=rel_a_rdd.join(rel_b_rdd)
#becareful!!


# In[35]:


rel_c_rdd.collect()


# In[36]:


rel_join_rdd=rel_a_rdd.map(lambda x: (x.k, (x.a1,x.a2))).join(rel_b_rdd.map(lambda x: (x.k,(x.b1,x.b2))))


# In[37]:


rel_join_rdd.collect()


# In[38]:


t1=rel_join_rdd.take(1)


# In[39]:


type(t1)


# In[40]:


t1


# In[41]:


t1[0]


# In[42]:


t1[0][1]


# In[43]:


t1[0][1][1][1]

