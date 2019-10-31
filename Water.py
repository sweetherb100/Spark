
# coding: utf-8

# In[1]:


import collections

Perforation = collections.namedtuple("Perforation", "SITE_CODE PERFORATION_TOP_MSRMNT                                         PERFORATION_BOTTOM_MSRMNT LATITUDE LONGITUDE")

Site = collections.namedtuple("Site", "SITE_CODE BASIN_DESC COUNTY_NAME SITE_USE_DESC")

perforation1=Perforation("379583N1219669W001",46,110,38.559,-122.5215)
perforation2=Perforation("379632N1219700W001", 206, 223, 38.5292,-122.5015)
perforation3=Perforation("379178N1216700W001",50,150,35.6347,-117.7226)

site1=Site("379583N1219669W001","Enterprise","Napa","Residential")
site2=Site("379632N1219700W001","Millville","Kern","Irrigation")
site3=Site("379178N1216700W001","South Battle Creek", "Santa Barbara", "Unknown")

perforationRDD=sc.parallelize([perforation1, perforation2, perforation3])
siteRDD=sc.parallelize([site1, site2, site3])



# In[3]:


#provide  name, latitude and longitude of the site with deepest deep

max_depth=perforationRDD.map(lambda x: x.PERFORATION_BOTTOM_MSRMNT -
                              x.PERFORATION_TOP_MSRMNT).max()
print max_depth


#determine max_depth_site
max_depth_site = perforationRDD.filter(lambda x: x.PERFORATION_BOTTOM_MSRMNT -
                                        x.PERFORATION_TOP_MSRMNT == max_depth).\
                                        map(lambda x: x.SITE_CODE).collect()
print max_depth_site

siteRDD.filter(lambda x: x.SITE_CODE in max_depth_site).collect()



# In[5]:


siteRDD.map(lambda s: (s.SITE_CODE,s.BASIN_DESC)).join(perforationRDD.        map(lambda p: (p.SITE_CODE,(p.LATITUDE,p.LONGITUDE)))).        filter(lambda x: x[0] in max_depth_site).map(lambda x: x[1]).collect()



# In[6]:


#provide name and county of sites devoted to Irrigation
siteRDD.filter(lambda x: x[-1]=="Irrigation").map(lambda x: x[1:3]).collect()


# In[7]:


perforationDF = perforationRDD.toDF(["SITE_CODE", "PERFORATION_TOP_MSRMNT",                                      "PERFORATION_BOTTOM_MSRMNT", "LATITUDE", "LONGITUDE"])
siteDF = siteRDD.toDF(["SITE_CODE", "BASIN_DESC", "COUNTY_NAME", "SITE_USE_DESC"])


# In[28]:


#provide  name, latitude and longitude of the site with deepest deep

max_depth_2=perforationDF.select(perforationDF.PERFORATION_BOTTOM_MSRMNT - perforationDF.PERFORATION_TOP_MSRMNT).groupBy().max().collect()[0][0]
print max_depth_2


#determine max_depth_site
max_depth_site_2 = perforationDF.filter(perforationDF.PERFORATION_BOTTOM_MSRMNT -                                        perforationDF.PERFORATION_TOP_MSRMNT == max_depth_2).select("SITE_CODE").collect()[0][0]
print max_depth_site_2

max_depth_site_DF=siteDF.filter(siteDF.SITE_CODE == max_depth_site_2)
max_depth_site_DF.collect()




# In[30]:


max_depth_site_DF.join(perforationDF,max_depth_site_DF.SITE_CODE==perforationDF.SITE_CODE).select("BASIN_DESC","LATITUDE","LONGITUDE").collect()


# In[32]:


#provide name and county of sites devoted to Irrigation
siteDF.filter("SITE_USE_DESC=='Irrigation'").select("BASIN_DESC", "COUNTY_NAME").collect()

