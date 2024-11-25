#!/usr/bin/env python
# coding: utf-8

# ## news_sentiment_analysis
# 
# New notebook

# **News Sentiment Analysis**

# In[1]:


# We use the final table to get the sentiment analysis
df = spark.sql("SELECT * FROM bing_lake_db.tbl_latest_news")
display(df)


# In[2]:


# We import a pre-trained model called AnalyzeText from synpase ml library
import synapse.ml.core
from synapse.ml.services import AnalyzeText


# In[3]:


# We use the description column to get the sentiment analysis
model = (AnalyzeText()
        .setTextCol("description")
        .setKind("SentimentAnalysis")
        .setOutputCol("response")
        .setErrorCol("error")

)


# In[5]:


# We apply the configured model to our DataFrame
result = model.transform(df)
display(result)


# In[7]:


# Create Sentiment column
from pyspark.sql.functions import col
sentiment_df = result.withColumn("sentiment", col("response.documents.sentiment"))
display(sentiment_df)


# In[8]:


# We delete the error and responde columns
sentiment_df_final = sentiment_df.drop("error", "response")
display(sentiment_df_final)


# In[9]:


# We write the final dataframe to the lakehouse in a delta table forma using incremental load type 1

from pyspark.sql.utils import AnalysisException

try:


    table_name = "bing_lake_db.tbl_sentiment_analysis"

    sentiment_df_final.write.format("delta").saveAsTable(table_name)


except AnalysisException:


    print("Table already exists")

    sentiment_df_final.createOrReplaceTempView("vw_sentiment_df_final")

    spark.sql(f""" MERGE INTO {table_name} target_table

                   USING vw_sentiment_df_final source_view

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

