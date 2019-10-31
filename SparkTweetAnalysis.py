
# coding: utf-8

# In[1]:

import collections



# In[2]:


Tweet = collections.namedtuple("Tweet", "num date time text")
ClassifiedTweet = collections.namedtuple("ClassifiedTweet", "num sentiment")


# In[3]:

# Analyse a text and detect if it is positive negative or neutral 
def sentiment(s): 
    positive = ("like", "love", "good", "great", "happy","cool", "amazing")
    negative = ("hate", "bad", "stupid")
    st=0;
    words = s.split(" ")
    for p in positive:
        for w in words:
            if p==w: 
                st = st+1
    num_neg=len(filter(lambda w: w in negative,words))
    
    st=st-num_neg
    if(st>0):
        return "positive"
    elif(st<0):
        return "negative"
    else:
        return "neutral"
    


# In[4]:

tweet1= Tweet(1,"22/06/2016","08:00:00","I love the new phone by YYYY")
tweet2= Tweet(2,"22/06/2016","08:10:00","The new camera by ZZZZ is amazing")
tweet3 =Tweet(3,"23/06/2016","08:30:00","""I heard about the strike but it is 
                unbelivable we don’t move for more than one hour. I hate traffic jams""")
tweetsRDD=sc.parallelize([tweet1,tweet2,tweet3])
#[ClassifiedTweet(num=1, sentiment='positive'), 
# ClassifiedTweet(num=2, sentiment='positive'), 
# ClassifiedTweet(num=3, sentiment='negative')]


# In[5]:

classifiedTweetsRDD=tweetsRDD.map(lambda t: ClassifiedTweet(t.num,sentiment(t.text)))
print classifiedTweetsRDD.collect()


# In[6]:

t1=tweetsRDD.map(lambda t:(t.num, t.date)).join(classifiedTweetsRDD.map(lambda t: (t.num, t.sentiment)))
print t1


# In[7]:

for (num, (date, sentiment)) in t1.toLocalIterator():
    print("%d %s %s" % (num, date, sentiment))

