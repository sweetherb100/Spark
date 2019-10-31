
# coding: utf-8

# In[39]:


import collections

Question = collections.namedtuple("Question", "id id_user title text keywords views votes")
Answer = collections.namedtuple("Answer", "id id_question id_user text")
User = collections.namedtuple("User", "id reputation profile")

q1= Question (1,1,"Cassandra Upsert not working on conditional writes",
              """I made a conditional insert (if not exists) \\\
              statement using DataStax java driver but it doesn't work""",
              "Java Cassandra DataStax", 1, 0)
q2= Question (2,1,"New Spark 2.2 Cassandra Connector",
              """ Tried to run the new connector to Spark 2.2 got error code 99129
              who can be of help?""",
              "Spark Cassandra", 2, 3)
u1= User(1, 1, "I'm an indipendent programmer, 8 years expertise in Java dev");
u2= User(2, 5, "I'm Matei, Spark creator");
u3= User(3, 5, "I'm Guido, Python benevolent dictator");

a1= Answer(1,1,2,"I think there is still a problem in DataStax connector, try to use the one at this link XXX")
a2= Answer(2,2,2,"Did you check server IP and Scala version?")
a3= Answer(3,2,3,"I think you are using Python 2.7, while the new API is for Python 3.0")
questionsRDD=sc.parallelize([q1,q2])
usersRDD=sc.parallelize([u1,u2,u3])
answersRDD=sc.parallelize([a1,a2,a3])


# In[40]:


#select questions keywords

def get_keywords(s):
    return s.split(" ")


keywordsRDD=questionsRDD.flatMap(lambda q: get_keywords(q.keywords)).distinct()

keywordsRDD.collect()


# In[41]:


#Select the power users, i.e., the users with the largest reputation providing also their profile
max_reputation=usersRDD.map(lambda u: u.reputation).max()
print max_reputation

power_usersRDD=usersRDD.filter(lambda u: u.reputation==max_reputation).map(lambda u:(u.id,u.profile))
power_usersRDD.collect()



# In[42]:


#Select the keywords of the questions answered by the power users

#Select power users' answers first
power_users_ID=power_usersRDD.map(lambda u: u[0]).collect()
power_answersRDD=answersRDD.filter(lambda a: a.id_user in power_users_ID)
power_answersRDD.collect()



# In[43]:


power_answersRDD_questions_IDs=power_answersRDD.map(lambda a: a.id_question).distinct().collect() 
print power_answersRDD_questions_IDs

print questionsRDD.filter(lambda q: q.id in power_answersRDD_questions_IDs).flatMap(lambda q: get_keywords(q.keywords)).distinct().collect()




# In[73]:


#provide questions and answers text
questionsRDD.map(lambda q: (q.id, q.text)).join(answersRDD.map(lambda a: (a.id_question, a.text))).map(lambda x: x[1]).collect()





# In[74]:


questionsRDD.join(answersRDD).map(lambda x: x[1]).collect()




# In[45]:


questionsDF = questionsRDD.toDF(["id", "id_user", "title", "text", "keywords", "views", "votes"])
usersDF = usersRDD.toDF(["id", "reputation", "profile"])
answersDF = answersRDD.toDF(["id", "id_question", "id_user", "text"])


# In[66]:


#Select the power users, i.e., the users with the largest reputation providing also their profile
max_reputation_2=usersDF.groupBy().max("reputation").distinct().collect()[0][0]
print max_reputation_2

power_usersDF=usersDF.filter(usersDF.reputation==max_reputation_2).select("id","profile")
power_usersDF.collect()




# In[76]:


#Select the keywords of the questions answered by the power users
print questionsDF.count()
power_answersDF = answersDF.join(power_usersDF, answersDF.id_user == power_usersDF.id)

power_answersDF.collect()



# In[85]:


power_answersDF_questions_IDs=power_answersDF.select("id_question","text").distinct()


power_questions_answerDF = questionsDF.join(power_answersDF_questions_IDs,power_answersDF_questions_IDs.id_question==questionsDF.id)
power_questions_answerDF.collect()





# In[89]:


power_questions_answerDF.select("keywords").distinct().collect()

