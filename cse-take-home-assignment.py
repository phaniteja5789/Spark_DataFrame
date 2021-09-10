#!/usr/bin/env python
# coding: utf-8

# d-sandbox
# 
# <div style="text-align: center; line-height: 0; padding-top: 9px;">
#   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# </div>

# # CSE Coding Assignment
# ## Instructions
# 
# - Please answer all questions
# - You can use any language you wish (e.g. Python, Scala, SQL...)
# - Several Markdown cells require completion. Please edit the Markdown cells to include your answer.
# - Your final notebook should compile without errors when you click "Run All"
# 
# **Please do not publish questions. This is a confidential assignment.**
# 
# ### Creating a Cluster
# 
# You will need to create a Databricks Cluster. More information on this process is available here: https://docs.databricks.com/user-guide/clusters/create.html

# ## Getting Started
# 
# **REQUIRED:** Run the following cells exactly as written to retrieve the necessary Coding Assignment Data Sets from Amazon S3.

# In[ ]:


get_ipython().run_line_magic('sh', "curl --remote-name-all 'https://files.training.databricks.com/assessments/cse-take-home/{covertype,kafka,treecover,u.data,u.item}.csv'")


# In[ ]:


dbutils.fs.cp("file:/databricks/driver/covertype.csv", "dbfs:/FileStore/tmp/covertype.csv")
dbutils.fs.cp("file:/databricks/driver/kafka.csv", "dbfs:/FileStore/tmp/kafka.csv")
dbutils.fs.cp("file:/databricks/driver/treecover.csv", "dbfs:/FileStore/tmp/treecover.csv")
dbutils.fs.cp("file:/databricks/driver/u.data.csv", "dbfs:/FileStore/tmp/u.data.csv")
dbutils.fs.cp("file:/databricks/driver/u.item.csv", "dbfs:/FileStore/tmp/u.item.csv")


# ## Part 1: Reading and Parsing Data
# 
# ### Question 1:  Code Challenge - Load a CSV
# 
# - Load the CSV file at `dbfs:/FileStore/tmp/nl/treecover.csv` into a DataFrame.
# - Use Apache Spark to read in the data, assigned to the variable `treeCoverDF`.
# - Your method to get the CSV file into Databricks isn't graded. We are only concerned with how you use Spark to parse and load the actual data. 
# - Please use the `inferSchema` option.

# In[ ]:


# YOUR CODE HERE

#Reading a CSV File from dbfs FileStore

treeCoverDF=spark.read.csv('dbfs:/FileStore/tmp/treecover.csv',header=True,inferSchema=True)


# In[ ]:


treeCoverDF.show()


# ### Question 2:  Code Challenge - Print the Schema
# 
# Use Apache Spark to display the Schema of the `treeCoverDF` Dataframe.

# In[ ]:


# YOUR CODE HERE

#treeCoverDF.printSchema()

treeCoverDF.printSchema()


# ### Question 3:  Code Challenge - Rows & Columns
# 
# Use Apache Spark to display the number of rows and columns in the DataFrame.

# In[ ]:


# YOUR CODE HERE

#Rows are calculated using treeCoverDF.count() it returns no of rows

#Columns are using treeCoverDF.columns it returns a List len(treeCoverDF.columns) returns the number of columns
from pyspark.sql.types import StructType,StructField,IntegerType
rows=treeCoverDF.count()
columns=len(treeCoverDF.columns)
rows_columns=[(rows,columns)]
schema_1=StructType(
  [
    StructField("ROWS",IntegerType(),False),
    StructField("COLUMNS",IntegerType(),False)
  ]
)
spark.createDataFrame(rows_columns,schema=schema_1).show()


# #Part 2: Analysis

# ### Question 4:  Code Challenge - Summary Statistics for a Feature
# 
# Use Apache Spark to answer these questions about the `treeCoverDF` DataFrame:
# - What is the range - minimum and maximum - of values for the feature `elevation`?
# - What are the mean and standard deviation of the feature `elevation`?

# In[ ]:


# YOUR CODE HERE

#Used Agg function on the DataFrame and calculated the Range=Maximum-Minimum
from pyspark.sql import functions as F
statisticts_treeCoverDF=treeCoverDF.agg(F.min("Elevation"),F.max("Elevation"),F.mean("Elevation"),F.stddev("Elevation"))
statisticts_treeCoverDF=statisticts_treeCoverDF.withColumnRenamed("min(Elevation)","Minimum").withColumnRenamed("max(Elevation)","Maximum").withColumnRenamed("avg(Elevation)","Mean").withColumnRenamed("stddev_samp(Elevation)","StandardDeviation")
statisticts_treeCoverDF=statisticts_treeCoverDF.withColumn("Range",statisticts_treeCoverDF.Maximum-statisticts_treeCoverDF.Minimum)
statisticts_treeCoverDF.show()


# ### Answer #4:
# 
# - Min `elevation`: 1863
# - Max `elevation`: 3849
# - Mean `elevation`: 2749.322
# - Standard Deviation of `elevation`: 417.678

# ### Question 5:  Code Challenge - Record Count
# 
# Use Apache Spark to answer the following question:
# - How many entries in the dataset have an `elevation` greater than or equal to 2749.32 meters **AND** a `Cover_Type` of 1 or 2?

# In[ ]:


# YOUR CODE HERE
#First Evaluates treeCoverDF["Elevation"] > = 2749.32
#Second Evaluates treeCoverDF["Cover_Type"] == 1
#third Evaluates treeCoverDF["Cover_Type"] == 2
#Evaluates all combined results
#Returns no of records that matches
treeCoverDF.filter((treeCoverDF["Elevation"] >= 2749.32) & ((treeCoverDF["Cover_Type"]==1) | (treeCoverDF["Cover_Type"]==2))).count()


# ### Question 6: Code Challenge - Compute a Percentage
# 
# Use Apache Spark to answer the following question:
# - What percentage of entries with `Cover_Type` 1 or 2 have an `elevation` at or above 2749.32 meters?

# In[ ]:


# YOUR CODE HERE

# (Condition Statisfied)/(Total_Entries)*100

condition_satisfied=treeCoverDF.filter((treeCoverDF["Elevation"] >= 2749.32) & ((treeCoverDF["Cover_Type"] == 1) | (treeCoverDF["Cover_Type"] == 2))).count()
total_entries=treeCoverDF.count()
percent=(condition_satisfied*100)/(total_entries)
percent


# ### Question 7: Code Challenge - Visualize Feature Distribution
# 
# Use any [visualization tool available in the Databricks Runtime](https://docs.databricks.com/user-guide/visualizations/index.html) to generate the following visualization:
# 
# - a bar chart that helps visualize the distribution of different Wilderness Areas in our dataset

# In[ ]:


# YOUR CODE HERE
display(treeCoverDF.select('Wilderness_Area'))


# ### Question 8: Code Challenge - Visualize Average Elevation by Cover Type 
# 
# Use any [visualization tool available in the Databricks Runtime](https://docs.databricks.com/user-guide/visualizations/index.html) to generate the following visualization:
# 
# - a bar chart showing the average elevation of each cover type with string labels for cover type
# 
# **NOTE: you will need to match the integer values in the column `treeCoverDF.Cover_Type` to the string values in `dbfs:/FileStore/tmp/nl/covertype.csv` to retrieve the Cover Type Labels. It is recommended to use an Apache Spark join.**

# In[ ]:


# YOUR CODE HERE
coverType_str=spark.read.csv('dbfs:/FileStore/tmp/covertype.csv',header=True)


# In[ ]:


coverType_str.show()


# In[ ]:


treeCover_numbers=treeCoverDF.select(treeCoverDF.Elevation,treeCoverDF.Cover_Type)
treeCover_numbers=treeCover_numbers.groupby("Cover_Type").avg("Elevation")
treeCover_numbers=treeCover_numbers.withColumnRenamed("Cover_Type","cover_type_key")
treeCover_numbers=treeCover_numbers.join(coverType_str,on="cover_type_key",how="inner")
display(treeCover_numbers.select(["cover_type_label","avg(Elevation)"]))


# In[ ]:





# #Part 3: Data Ingestion, Cleansing, and Transformations

# ## Instructions 
# 
# This is a multi-step, data pipeline question in which you need to achieve a few objectives to build a successful job.
# 
# ### Data Sets
# 
# #### `u.data.csv`
# 
# - The full u data set, 100000 ratings by 943 users on 1682 items. 
# - Each user has rated at least 20 movies.  
# - Users and items are numbered consecutively from 1. 
# - The data is randomly ordered. 
# - This is a tab separated file consisting of four columns: 
#    - user id 
#    - movie id 
#    - rating 
#    - date (unix seconds since 1/1/1970 UTC)
# 
# #### Desired schema
# 
# - `user_id INTEGER`
# - `movie_id INTEGER`
# - `rating INTEGER`
# - `date DATE `
# 
# #### `u.item.csv`
# 
# - This is a `|` separated file consisting of six columns:
#    - movie id
#    - movie title
#    - release date
#    - video release date
#    - IMDb URL
#    - genre
# - movie ids in this file match movie ids in `u.data`.
# 
# #### Desired schema
# 
# - `movie_id INTEGER`
# - `movie_title STRING`

# ### Question 9:  Code Challenge - Load DataFrames
# 
# Use Apache Spark to perform the following:
# 1. define the correct schemas for each Data Set to be imported as described above  
#    **note:** 
#       - for `u.data.csv`, `date` *must* be stored using `DateType` with the format `yyyy-MM-dd`
#       - you may need to ingest `timestamp` data using `IntegerType`
#       - be sure to drop unneccesary columns for `u.item.csv`
# 1. import the two files as DataFrames names `uDataDF` and `uItemDF` using the schemas you defined and these paths:
#    - `dbfs:/FileStore/tmp/u.data.csv`
#    - `dbfs:/FileStore/tmp/u.item.csv`
# 1. order the `uDataDF` DataFrame by the `date` column
# 
# **NOTE:** Please display the DataFrames, `uDataDF` and `uItemDF` after loading.

# #### `uDataDF`

# In[ ]:


# YOUR CODE HERE
from pyspark.sql.types import StructType,StructField,DateType,IntegerType
from pyspark.sql import functions as F

schema_data=StructType(
[
  StructField("user_id",IntegerType(),True),
  StructField("movie_id",IntegerType(),True),
  StructField("rating",IntegerType(),True),
  StructField("date",DateType(),True)
]
)

uDataDF=spark.read.csv('dbfs:/FileStore/tmp/u.data.csv',header=False,inferSchema=False,sep='\t')
uDataDF=uDataDF.withColumnRenamed("_c0","user_id").withColumnRenamed("_c1","movie_id").withColumnRenamed("_c2","rating").withColumnRenamed("_c3","date")
uDataDF=uDataDF.withColumn("date",F.from_unixtime(uDataDF.date,format="yyyy-MM-dd").cast(DateType()))
uDataDF=uDataDF.withColumn("user_id",uDataDF.user_id.cast(IntegerType())).withColumn("movie_id",uDataDF.movie_id.cast(IntegerType())).withColumn("rating",uDataDF.rating.cast(IntegerType()))
uDataDF.printSchema()


# In[ ]:


uDataDF.show()


# In[ ]:


uDataDF=uDataDF.orderBy("date")
uDataDF.show()


# #### `uItemDF`

# In[ ]:


# YOUR CODE HERE
uItemDF=spark.read.csv('dbfs:/FileStore/tmp/u.item.csv',header=False,sep='|')['_c0','_c1']


# In[ ]:


uItemDF=uItemDF.withColumnRenamed("_c0","movie_id").withColumnRenamed("_c1","movie_title")


# In[ ]:


uItemDF.printSchema()


# In[ ]:


uItemDF=uItemDF.withColumn("movie_id",uItemDF.movie_id.cast(IntegerType()))


# In[ ]:


uItemDF.printSchema()


# In[ ]:


uItemDF.show()


# ### Question 10:  Code Challenge - Perform a Join
# 
# Use Apache Spark to do the following:
# - join `uDataDF` and `uItemDf` on `movie_id` as a new DataFrame called `uMovieDF`  
#    **note:** make sure you do not create duplicate `movie_id` columns
#    
# **NOTE:** Please display the DataFrame `uMovieDF`.

# In[ ]:


# YOUR CODE HERE
uMovieDF=uDataDF.join(uItemDF,on="movie_id",how="inner")
display(uMovieDF)


# ### Question 11:  Code Challenge - Perform an Aggregation
# 
# Use Apache Spark to do the following:
# 1. create an aggregate DataFrame, `aggDF` by
#   1. extracting the year from the `date` (of the review)
#   1. getting the average rating of each film per year as a column named `average_rating`
#   1. ordering descending by year and average rating
# 1. write the resulting dataframe to a table named "movie_by_year_average_rating" in the Default database  
#    **note:** use `mode(overwrite)` 
# 
# #### Desired Schema
# The schema of you resulting DataFrame should be:
# - `year INTEGER`
# - `movie_title STRING`
# - `average_rating DOUBLE`
# 
# **NOTE:** Please display the DataFrame `aggDF`.

# In[ ]:


uMovieDF.columns


# In[ ]:


aggDF=uMovieDF.select([F.year(uMovieDF.date),uMovieDF.movie_title,uMovieDF.rating])
aggDF=aggDF.withColumnRenamed("year(date)","year")


# In[ ]:


aggDF=aggDF.groupBy("year","movie_title").avg("rating")


# In[ ]:


aggDF=aggDF.withColumnRenamed("avg(rating)","average_rating")


# In[ ]:


aggDF=aggDF.orderBy(["year","average_rating"],ascending=False)


# In[ ]:


aggDF.createTempView("movie_by_year_average_rating")


# In[ ]:


aggDF.writeTo("movie_by_year_average_rating").createOrReplace()


# In[ ]:


aggDF.printSchema()


# ## Part 4: Fun with JSON
# 
# JSON values are typically passed by message brokers such as Kafka or Kinesis in a string encoding. When consumed by a Spark Structured Streaming application, this json must be converted into a nested object in order to be used.
# 
# Below is a list of json strings that represents how data might be passed from a message broker.
# 
# **Note:** Make sure to run the cell below to retrieve the sample data.

# In[ ]:


get_ipython().run_line_magic('python', '')


sampleJson = [
 ('{"user":100, "ips" : ["191.168.192.101", "191.168.192.103", "191.168.192.96", "191.168.192.99"]}',), 
 ('{"user":101, "ips" : ["191.168.192.102", "191.168.192.105", "191.168.192.103", "191.168.192.107"]}',), 
 ('{"user":102, "ips" : ["191.168.192.105", "191.168.192.101", "191.168.192.105", "191.168.192.107"]}',), 
 ('{"user":103, "ips" : ["191.168.192.96", "191.168.192.100", "191.168.192.107", "191.168.192.101"]}',), 
 ('{"user":104, "ips" : ["191.168.192.99", "191.168.192.99", "191.168.192.102", "191.168.192.99"]}',), 
 ('{"user":105, "ips" : ["191.168.192.99", "191.168.192.99", "191.168.192.100", "191.168.192.96"]}',), 
]


# ### Question 12:  Code Challenge - Count the IPs
# 
# Use any coding techniques known to you to parse this list of JSON strings to answer the following question:
# - how many occurrences of each IP address are in this list?
# 
# #### Desired Output
# Your results should be this:
# 
# 
# | ip | count |
# |:-:|:-:|
# | `191.168.192.96` | `3` |
# | `191.168.192.99` | `6` |
# | `191.168.192.100` | `2` |
# | `191.168.192.101` | `3` |
# | `191.168.192.102` | `2` |
# | `191.168.192.103` | `2` |
# | `191.168.192.105` | `3` |
# | `191.168.192.107` | `3` |
# 
# **NOTE:** The order of your results is not important.

# In[ ]:


# YOUR CODE HERE
import json
ips_dic={}
def process(sampleJson):
  dict_obj=json.loads(sampleJson[0])
  for i in dict_obj['ips']:
    if i in ips_dic.keys():
      ips_dic[i]+=1
    else:
      ips_dic[i]=1
list(map(process,sampleJson))
final_list=[]
for k,v in ips_dic.items():
  final_list.append((k,v))
spark.createDataFrame(final_list,["ip","count"]).show()


# ## Part 5: The Databricks API

# ### Question 13: Conceptual Question - the Databricks API
# 
# In 4-5 sentences, please explain what the Databricks API is used for at a high-level.

# ### Answer:
# Databricks API is used to perform the big data transformations by loading the data from various sources like aws s3,amazon redshift.
# It is also used to load the data from Local File System and has the capability of creation of tables and directly load the data into tables.
# Databricks API has by default SparkContext Object so that there is no need to explicitly specify the SparkContext Object
# 
# `EDIT THIS MARKDOWN CELL WITH YOUR REPLY`

# ### Question 14: Conceptual Question - Explain an API Call
# 
# In 4-5 sentences, please explain what this API call. Be sure to discuss some key attributes about the cluster.
# 
# ```
# $ curl -n -X POST -H 'Content-Type: application/json'                      \
#   -d '{                                                                     \
#   "cluster_name": "high-concurrency-cluster",                               \
#   "spark_version": "4.2.x-scala2.11",                                       \
#   "node_type_id": "i3.xlarge",                                              \
#   "spark_conf":{                                                            \
#         "spark.databricks.cluster.profile":"serverless",                    \
#         "spark.databricks.repl.allowedLanguages":"sql,python,r"             \
#      },                                                                     \
#      "aws_attributes":{                                                     \
#         "zone_id":"us-west-2c",                                             \
#         "first_on_demand":1,                                                \
#         "availability":"SPOT_WITH_FALLBACK",                                \
#         "spot_bid_price_percent":100                                        \
#      },                                                                     \
#    "custom_tags":{                                                          \
#         "ResourceClass":"Serverless"                                        \
#      },                                                                     \
#        "autoscale":{                                                        \
#         "min_workers":1,                                                    \
#         "max_workers":2                                                     \
#      },                                                                     \
#   "autotermination_minutes":10                                              \
# }' https://dogfood.staging.cloud.databricks.com/api/2.0/clusters/create '
# ```

# ### Answer:
# The above API defines the configuration 
# cluster_name defines the name of the cluster on which spark is running
# spark_version defines the version of spark used
# spark.databricks.cluster.profile defines the serverless which means the application runs on serverless architecture
# spark.databricks.repl.allowedLanguages the programming languages that can be used in databricks 
# zone_id defines the zone_id where the Nodes and the cluster is running
# autoscale based on the queries it will dynamically scale up and scale down based on requests
# min_workers defines number of executors =1
# max_workers defines maximum number of executors=2 when automatically scales
# autotermination_minutes =10 will automatically terminate the workers execution
# 
# `EDIT THIS MARKDOWN CELL WITH YOUR REPLY`

# ## Part 6: Security

# ### Question 15: Conceptual Question - Security on Databricks
# 
# Using the Databricks Documentation, what would you recommend to a Databricks and AWS customer for **securely** storing and accessing their data.

# ### Answer:
# AWS provides a huge number of services and when it comes to security AWS maintain IAM which means Identity Access Management which keeps track of people access policies and also has cloudtrail and many more services if there is any spike in the activity AWS keeps track of logs in order to keep the customer information safe.
# `EDIT THIS MARKDOWN CELL WITH YOUR REPLY`

# # This is the end of the official test. Bonus below!

# ## Part 7: Bonus: Data Science & Machine Learning

# ### Question 16: Conceptual Question - A Skewed Feature
# 
# One of these lines is the *mean* of this feature. The other is the *median*. Which of these lines is the **mean** - the red line or the black line?
# 
# <img width=400px src=https://www.evernote.com/l/AAEycL6CQ0hLi5V5pIo91Ko-Pfk2i0AnGyMB/image.png>

# ### Answer:
# The above distribution is Assymetric Distribution.
# And it is Negatively Skewed since the tail of left side is very large.
# In Case of Negative Skewed Mean comes before Median.
# So Red Line is Mean.
# `EDIT THIS MARKDOWN CELL WITH YOUR REPLY`

# ### Question 17: Conceptual Question - Exploratory Data Analysis
# 
# The plots below show the distribution of home selling prices differentiated by a few categorical features. Based on these plots, **which of these categorical features** - Property Type, Exterior Quality, or Month Sold - would you expect to be most associated with Price? **Why**?
# 
# <img width=1600px src=https://www.evernote.com/l/AAHulkcc20hHSJV6D1udKiwSDCN0S6oV_5YB/image.png>

# ### Answer:
# 
# `EDIT THIS MARKDOWN CELL WITH YOUR REPLY`

# ### Question 18: Conceptual Question - Analyze Model Performance
# 
# Consider the following results for a decision tree model against training and testing data sets:
# 
# `decision tree regression - train r2 score: 0.9944`  
# `decision tree regression - test r2 score:  0.3119`
# 
# 
# What is your assessment of this model?

# ### Answer:
# The Model is trained with the data that is overfitted because of that the Test Data accuracy is very less.
# `EDIT THIS MARKDOWN CELL WITH YOUR REPLY`

# ### Question 19: Conceptual Question - Model Selection
# 
# A series of models has been built using the same training data, but each with a subset of features.
# 
# Consider the following results for a series of logistic regression models and then answer this question:
# 
# - Which model would you choose and why?
# - What other things would you want to look at and why?
# 
# | model number | feature subset | logistic regression test accuracy|
# |:-:|:-:|:-:|
# | 1| feat_1 |	 0.631|
# | 2| feat_2 |	 0.552|
# | 3| feat_3 |	 0.868|
# | 4| feat_4 |	 0.868|
# | 5| feat_1, feat_2 |	 0.657|
# | 6| feat_1, feat_3 |	 0.947|
# | 7| feat_1, feat_4 |	 0.921|
# | 8| feat_2, feat_3 |	 0.947|
# | 9| feat_2, feat_4 |	 0.973|
# | 10| feat_3, feat_4 |	 0.947|
# | 11| feat_1, feat_2, feat_3 |	 0.947|
# | 12| feat_1, feat_2, feat_4 |	 0.947|
# | 13| feat_1, feat_3, feat_4 |	 0.947|
# | 14| feat_2, feat_3, feat_4 |	 0.973|
# | 15| feat_1, feat_2, feat_3, feat_4 |	 0.973|

# ### Answer:
# 
# `EDIT THIS MARKDOWN CELL WITH YOUR REPLY`

# -sandbox
# &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# <br/>
# <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
