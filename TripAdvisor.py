
# coding: utf-8

# In[1]:


import collections

User = collections.namedtuple("User", "id nickname")
Place = collections.namedtuple("Place", "id name description classification")
Review = collections.namedtuple("Review", "id id_user id_place text rating")


user1=User(1,"barack2009")
user2=User(2,"donald2017")

place1 = Place(1,"Daniel", "NYC rank 1 French Restaurant", "Fine dining,French")
place2 = Place(2,"Pizza Suprema","Owned by the Same Italian Family for over 45 years!", "Italian,Pizza,Fast Food")
place3 = Place(3, "Los Tacos No. 1","LOS TACOS No.1  was created after 3 close friends from Tijuana decided to bring the authentic Mexican taco to the east coast", "Mexican,Latin,Fast Food")


review1 = Review(1,2,1,"Best restaurant in New York in my opinion. Only a few Michelin 3 stars in Paris are comparable",5)
review2 = Review(2,1,2,"Yummy fresh pizza! Ordered pizza to our room. As a warning - it is MASSIVE!!! Hey yummy and filling and everything tasted fresh",5)
review3 = Review(3,1,3,"Amazing, quick Mexican food 4. We had Los Tacos for lunch this past weekend. It was so authentic. Small menu, but it's ready in minutes. SOOO good!",4)
review4 = Review(4,1,3,"I went to the one on 43rd st. This was freaking good best steak taco I have ever had!!! So tasty! Just excellent!",5)

usersRDD=sc.parallelize([user1,user2])
placesRDD=sc.parallelize([place1,place2,place3])
reviewsRDD=sc.parallelize([review1,review2,review3,review4])





# In[2]:


# Select the  ratings for Fast Food (provide also Place name)

#Filter Fast Food restaurants
def filterClass(classification, criterion):
    return criterion in classification.split(",")

filteredPlacesRDD=placesRDD.map(lambda x: (x[0], (x[1], x[-1]) )).filter(lambda x: filterClass(x[-1][-1],"Fast Food"))
filteredPlacesRDD.collect()





# In[4]:


# Select ratings
ratingsRDD=reviewsRDD.map(lambda x: (x.id_place, x.rating) )
ratingsRDD.collect()

joinRDD=filteredPlacesRDD.join(ratingsRDD).map(lambda x: (x[-1][0][0],x[-1][-1]))
joinRDD.collect()


# In[6]:


usersDF=usersRDD.toDF(["id", "nickname"])
usersDF.show()


# In[7]:



placesDF=placesRDD.toDF(["id", "name", "description", "classification"])
placesDF.show()


# In[8]:


reviewsDF=reviewsRDD.toDF(["id", "id_user", "id_place", "text", "rating"])
reviewsDF.show()


# In[26]:


from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType
filterClass_udf=udf(lambda x: filterClass(x, "Fast Food"),BooleanType())

filteredPlacesDF = placesDF.filter(filterClass_udf(placesDF.classification))


# In[27]:


filteredPlacesDF.show()


# In[33]:


filteredPlacesDF.join(reviewsDF,reviewsDF.id_place==filteredPlacesDF.id).select(filteredPlacesDF.name,reviewsDF.rating).show()

