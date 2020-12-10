# Code Author:-
# Name: Shivam Gupta
# Net ID: SXG190040
# CS 6350.001 - Big Data Management and Analytics - F20 Assignment 3 Question 2


#Importing the required Libaries
from math import sqrt
import itertools
import sys
from pyspark import SparkConf, SparkContext
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from operator import add



# RMSE Calculation function
def RMSE_Evaluation(als_model, Ratings_Data, Data_Count):
    #Compute RMSE (Root Mean Squared Error).
    PREDICTIONS = als_model.predictAll(Ratings_Data.map(lambda R: (R[0], R[1])))
    Ratings_PREDICTIONS = PREDICTIONS.map(lambda R: ((R[0], R[1]), R[2])).join(Ratings_Data.map(lambda R: ((R[0], R[1]), R[2]))).values()
    return sqrt(Ratings_PREDICTIONS.map(lambda R: (R[0] - R[1]) ** 2).reduce(add) / float(Data_Count))


if __name__ == "__main__":
    conf = SparkConf().setAppName("Movies_Recommendation_ALS").set("spark.executor.memory", "2g")

    # Loading and parsing the RATINGS Data
    RATINGS_Data = sc.textFile("/FileStore/tables/ratings.dat")
    RATINGS_RDD = RATINGS_Data.map(lambda Line: Line.strip().split('::')).map(
        lambda Line: (float(Line[3]) % 10, (int(Line[0]), int(Line[1]), float(Line[2]))))


    Partitions_NUM = 4
    # Splitting the Data into 70% training(65% Actual Training and 5% Validation) and 30% Testing
    (Training_DATA, Validation_DATA, Testing_DATA) = RATINGS_RDD.randomSplit([0.65,0.05,0.3])
    Training_DATA = Training_DATA.values().repartition(Partitions_NUM).cache()
    Validation_DATA = Validation_DATA.values().repartition(Partitions_NUM).cache()
    Testing_DATA = Testing_DATA.values().cache()

    TRAIN_Data_Count = Training_DATA.count()
    VAL_Data_Count = Validation_DATA.count()
    TEST_Data_Count = Testing_DATA.count()
    print("Training_DATA: %d, Validation_DATA: %d, Testing_DATA: %d" %
        (TRAIN_Data_Count, VAL_Data_Count, TEST_Data_Count))

    RANKS_Vector = [4, 8]
    LAMBDAS_Vector = [0.1, 10.0]
    Iterations_Vector = [5, 10]
    Best_ALS_Model = None
    Best_RMSE_Value = float("inf")
    Best_RANK_Value = 0
    Best_LAMBDA_Value = -1.0
    Best_ITER_Value = -1
    for RANK_VAL, LAMBDA_VAL, ITER_VAL in itertools.product(RANKS_Vector, LAMBDAS_Vector, Iterations_Vector):
        ALS_MODEL = ALS.train(Training_DATA, RANK_VAL, ITER_VAL, LAMBDA_VAL)
        VAL_RMSE = RMSE_Evaluation(ALS_MODEL, Validation_DATA, VAL_Data_Count)
        print("RMSE (Validation_DATA) = %f for the ALS Model trained with parameters " % VAL_RMSE +
            "RANK_VAL = %d, LAMBDA_VAL = %.1f, and ITER_VAL = %d." % (
                RANK_VAL, LAMBDA_VAL, ITER_VAL))
        if (VAL_RMSE < Best_RMSE_Value):
            Best_ALS_Model = ALS_MODEL
            Best_RMSE_Value = VAL_RMSE
            Best_RANK_Value = RANK_VAL
            Best_LAMBDA_Value = LAMBDA_VAL
            Best_ITER_Value = ITER_VAL

        TEST_RMSE = RMSE_Evaluation(Best_ALS_Model, Testing_DATA, TEST_Data_Count)
        print("=====================================================================================================================================")
        print("The best Recommendation ALS model has been trained with RANK_VAL -> %d and LAMBDA_VAL -> %.1f, " % (Best_RANK_Value, Best_LAMBDA_Value)
            + "and with Iterations -> %d, the RMSE for the Testing_DATA set is %f." % (Best_ITER_Value, TEST_RMSE))

    print("The Final Accuracy Metric: Root Mean Square Error is: ", TEST_RMSE)