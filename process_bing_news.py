#!/usr/bin/env python
# coding: utf-8

# ## process_bing_news
# 
# New notebook

# **Data Transformation (Incremental Load)**

# In[1]:


# Read the json file as a dataframe
df = spark.read.option("multiline", "true").json("Files/bing-latest-news.json")
# df now is a Spark DataFrame containing JSON data from "Files/bing-latest-news.json".
display(df)


# In[2]:


# We ignore all the other columns and only keep the value column from the dataframe
df = df.select("value")
display(df)


# In[3]:


# Processing the value column
# All the news articles (objects) exist in a single row, so we need each article in each row 
from pyspark.sql.functions import explode
df_exploded = df.select(explode(df["value"]).alias("json_object"))


# In[4]:


# Let's display de new dataframe
display(df_exploded)


# In[5]:


# We convert all the json objects to one single json string list
json_list = df_exploded.toJSON().collect()
print(json_list)


# In[6]:


# Let's see only one article in the list
print(json_list[10])


# In[7]:


# Now we convert the json list to a dictionary so we can easily manipulate the information
import json 
news_json = json.loads(json_list[10])
print(news_json)


# In[8]:


# We can, for example, acces only to the description of the article
print(news_json['json_object']['description'])


# In[9]:


# Here we see all the information we need for one article
print(news_json['json_object']['name'])
print(news_json['json_object']['description'])
print(news_json['json_object']['url'])
print(news_json['json_object']['image']['thumbnail']['contentUrl'])
print(news_json['json_object']['provider'][0]['name'])
print(news_json['json_object']['datePublished'])



# In[10]:


# Here we can obtain the information for all the news articles with a for loop to iterate the json list
title = []
description = []
url = []
image = []
provider = []
datePublished = []

# Process each JSON object in the list
for json_str in json_list:
    try:
        # Parse the JSON string into a dictionary
        article = json.loads(json_str)

        if article["json_object"].get("image", {}).get('thumbnail', {}).get("contentUrl", {}):

           # Extract information from the dictionary
           title.append(article['json_object']['name'])
           description.append(article['json_object']['description'])
           url.append(article['json_object']['url'])
           image.append(article['json_object']['image']['thumbnail']['contentUrl'])
           provider.append(article['json_object']['provider'][0]['name'])
           datePublished.append(article['json_object']['datePublished'])

    except Exception as e:
     print(f"Error processing JSON object: {e}")


# In[11]:


# We can see the list with all the different titles
title


# In[12]:


# Now we can combine all the lists together and create a dataframe with a defined schema
from pyspark.sql.types import StructType, StructField, StringType

# Combine the lists
data = list(zip(title, description, url, image, provider, datePublished))

# Define schema
schema = StructType([
  StructField("title", StringType(), True),
  StructField("description", StringType(), True),
  StructField("url", StringType(), True),
  StructField("image", StringType(), True),
  StructField("provider", StringType(), True),
  StructField("datePublished", StringType(), True)  
])

# Create the DataFrame
df_cleaned = spark.createDataFrame(data, schema=schema)
display(df_cleaned) 


# In[13]:


# We convert the datePublished column from date time format to date
from pyspark.sql.functions import to_date, date_format
df_cleaned_final = df_cleaned.withColumn("datePublished",date_format(to_date("datePublished"), "dd-MM-yyyy"))
display(df_cleaned_final)


# In[14]:


# We write the final dataframe to the lakehouse in a delta table forma using incremental load type 1

from pyspark.sql.utils import AnalysisException
try:


    table_name = "bing_lake_db.tbl_latest_news"
    df_cleaned_final.write.format("delta").saveAsTable(table_name)

except AnalysisException:

    print("Table already exists")

    df_cleaned_final.createOrReplaceTempView("vw_df_cleaned_final")

    spark.sql(f"""  MERGE INTO {table_name} target_table
                    USING vw_df_cleaned_final source_view


                    ON source_view.url = target_table.url


                    WHEN MATCHED AND
                    source_view.title <> target_table.title OR
                    source_view.description <> target_table.description OR
                    source_view.image <> target_table.image OR
                    source_view.provider <> target_table.provider OR
                    source_view.datePublished <> target_table.datePublished


                    THEN UPDATE SET *

                    WHEN NOT MATCHED THEN INSERT *


    """)

