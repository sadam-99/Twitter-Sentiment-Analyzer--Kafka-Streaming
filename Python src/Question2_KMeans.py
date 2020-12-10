# Code Author:-
# Name: Shivam Gupta
# Net ID: SXG190040
# CS 6350.001 - Big Data Management and Analytics - F20 Assignment 3 Question 2


from math import sqrt
from numpy import array
import pandas as pd
from pyspark.sql.window import Window
from pyspark.sql.functions import col, rank
from pyspark.mllib.clustering import KMeans


Item_user_Data = sc.textFile("/FileStore/tables/itemusermat")
Movies_Data_DF = spark.read \
    .option("sep", "::") \
    .csv("/FileStore/tables/movies.dat") \
    .withColumnRenamed("_c0", "Movie_ID") \
    .withColumnRenamed("_c1", "Movie_Title") \
    .withColumnRenamed("_c2", "Movie_Genre") \
    .dropDuplicates()
Ratings_Data = Item_user_Data.map(lambda Line_Data: array([int(item) for item in Line_Data.split(' ')[1:]]))
Clusters_Num = 10
Kmeans_Model = KMeans.train(Ratings_Data, Clusters_Num, maxIterations=10, initializationMode="random")
 

# Kmeans Clustering Prediction function
def Kmeans_Cluster_Predict(Data_Points):
    Centroid = Kmeans_Model.predict(Data_Points[1:])
    return (Data_Points[0], Centroid)
 
Cluster_INFO = Item_user_Data.map(lambda Line_Data: array([int(item) for item in Line_Data.split(' ')])).map(lambda Data_Points: Kmeans_Cluster_Predict(Data_Points)).collect()
output_Columns = ["MovieID","Cluster_Num"]
output_df = pd.DataFrame(list(Cluster_INFO), columns = output_Columns)
Res_DF = spark.createDataFrame(output_df)
 
Results_DF = Res_DF.join(Movies_Data_DF, Movies_Data_DF.Movie_ID == Res_DF.MovieID, how="inner")
 
Partition_Window = Window.partitionBy(Results_DF['Cluster_Num']).orderBy(Results_DF['MovieID'].desc())
  
Top5_Results_DF = Results_DF.select('*', rank().over(Partition_Window).alias('rank')).filter(col('rank') <= 5)
Top5_Results_DF.show()