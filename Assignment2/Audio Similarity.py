###### DATA420 Assignment 2 Peng Shen (student ID: 57408055)
##### Audio Similarity

# Python and pyspark modules required
start_pyspark_shell -e 2 -c 1 -w 1 -m 1

import sys

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import Window
from operator import or_
from functools import reduce
from pretty import SparkPretty  # download pretty.py from LEARN and put in the same directory

# Required to allow the file to be submitted and run using spark-submit instead
# of using pyspark interactively

spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()
pretty = SparkPretty(limit=5)

# Compute suitable number of partitions
conf = sc.getConf()

N = int(conf.get("spark.executor.instances"))
M = int(conf.get("spark.executor.cores"))
partitions = 4 * N * M

# --------------------------------------------------------------
#  Q1 (a)
# Check the dimension of each audio feature dataset
for dataset_name in dataset_names:
    num_columns = len(datasets[dataset_name].columns)
    num_rows = datasets[dataset_name].count()
    print(dataset_name,num_columns, num_rows)
# msd-jmir-area-of-moments-all-v1.0             21 994623
# msd-jmir-lpc-all-v1.0                         21 994623
# msd-jmir-methods-of-moments-all-v1.0          11 994623
# msd-jmir-mfcc-all-v1.0                        27 994623
# msd-jmir-spectral-all-all-v1.0                17 994623
# msd-jmir-spectral-derivatives-all-all-v1.0    17 994623
# msd-marsyas-timbral-v1.0                     125 995001
# msd-mvd-v1.0                                 421 994188
# msd-rh-v1.0                                   61 994188
# msd-rp-v1.0                                 1441 994188
# msd-ssd-v1.0                                 169 994188
# msd-trh-v1.0                                 421 994188
# msd-tssd-v1.0                               1177 994188

# Load the 'methods-of-moments-all-v1' dataset
features = datasets["msd-jmir-methods-of-moments-all-v1.0"].repartition(partitions)
features.show(10)
print(pretty(features.head().asDict()))
features.printSchema()

# Descriptive statistics
numeric_features = features.columns[:-1]
feature_stats = features.select(numeric_features).describe().toPandas()
feature_stats.transpose()

# Correlation
import pandas as pd
from pyspark.ml.feature import VectorAssembler
from pyspark.mllib.stat import Statistics

df = features.drop("MSD_TRACKID")
#col_names = df.columns
rdd = df.rdd.map(lambda row: row[0:])
corr_matrix = Statistics.corr(rdd, method="pearson")
corr_df = pd.DataFrame(corr_matrix)
# corr_df.index, corr_df.columns = col_names, col_names
print(corr_df.to_string())
print(corr_df.style.background_gradient(cmap='coolwarm'))

# Another approach 
corr = features.toPandas().corr()
corr.to_csv('corr.csv')
# Execute code below in jupyter notebook
df = pd.read_csv('corr.csv') 
styled_df = df.style.background_gradient(cmap='coolwarm')
style.to_excel('corr.xlsx')

#-------------------------------------------------------------------
# Q1 (b)
# Load the genre dataset
# Load genre datasets
genre_schema = StructType([
    StructField("TRACK_ID", StringType(), True),
    StructField("GENRE", StringType(), True)
])
genres = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("delimiter", "\t")
    .option("inferSchema", "false")
    .schema(genre_schema)
    .load("hdfs:///data/msd/genre/msd-MAGD-genreAssignment.tsv")
    .repartition(partitions)
    .cache()
)
genres.show(5, False)
genres.count() #422714
print(pretty(genres.head().asDict()))

# Visualize the distribution of genres
import matplotlib.pyplot as plt

genre_distribution = (
    genres
    .groupBy('GENRE')
    .count()
    .orderBy('count', ascending=False)
    .toPandas()
    )

genre_distribution.index = genre_distribution.GENRE
plt.close()
plt.figure(dpi=300, figsize=(10, 10))
plt.title('Distribution of Genres', fontsize=30)
genre_distribution['count'].plot.barh()
plt.ylabel('Genre', fontsize = 20, rotation=90)
plt.xlabel('Count', fontsize = 20)
plt.yticks(fontsize=10)
plt.xticks(fontsize=11)
# plt.legend(loc='best', ncol=2, prop={'size': 6})
plt.savefig('genre_distribution.jpg')

# ------------------------------------------------------
# Q1 (c)
# First we need to remove the '' from MSD_TRACKID in features dataframe
features = (
    features
    .withColumn('TRACK_ID', substring(col('MSD_TRACKID'), 2, 18).cast(StringType()))
    .drop('MSD_TRACKID')
    )
genre_labeled_features = (
    features
    .join(
        genres, 
        on = 'TRACK_ID'
        )
    .cache()
    )
genre_labeled_features.show(10, False)
genre_labeled_features.printSchema()
genre_labeled_features.count() #420620

# ---------------------------------------------------------
# Q2 (a)
# Pre-processing
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler, PCA

numeric_var = [cols[0] for cols in genre_labeled_features.dtypes if cols[1] == 'double']
vecAssembler = VectorAssembler(inputCols=numeric_var, outputCol="vec_features")
scaler = StandardScaler(inputCol="vec_features", outputCol="scaled_features")
pca = PCA(k=5, inputCol="scaled_features", outputCol="features")
pipeline = Pipeline(stages=[vecAssembler, scaler, pca])

# Fit on training data
pipelineFit = pipeline.fit(genre_labeled_features)

# Process training and testing data
dataset = pipelineFit.transform(genre_labeled_features).select('features', 'GENRE')

# ---------------------------------------------------------
# Q2 (b)
# create a new column as label
data_binary = (
    dataset
    .withColumn('label', when(col('GENRE')=='Electronic', 1).otherwise(0))
    .cache()
    )
data_binary.groupBy('label').count().show()


# ---------------------------------------------------------
# Q2 (c)
# train and test split

# Define a helper function to check class balance
def print_class_balance(data, name):
    N = data.count()
    counts = data.groupBy("label").count().toPandas()
    counts["ratio"] = counts["count"] / N
    print(name)
    print(N)
    print(counts)
    print("")
    
# Random Split (not stratified)
trainingData, testingData = data_binary.randomSplit([0.8, 0.2])
trainingData.cache()
testingData.cache()
trainingData.groupBy('label').count().show()
testingData.groupBy('label').count().show()

# Stratified split (per-strata simple random sampling, which is not exact)
temp = data_binary.withColumn('id', monotonically_increasing_id())
trainingData = temp.sampleBy('label', fractions={0:0.8, 1:0.8})
trainingData.cache()
testingData = temp.join(trainingData, on = 'id', how='left_anti')
testingData.cache()

trainingData = trainingData.drop('id')
testingData = testingData.drop('id')
trainingData.groupBy('label').count().show()
testingData.groupBy('label').count().show()

print_class_balance(data_binary, 'features')
print_class_balance(trainingData, 'training')
print_class_balance(testingData, 'testing')

# Exact Stratified split using Window
temp = (
    data_binary
    .withColumn('id', monotonically_increasing_id())
    .withColumn('Random', rand())
    .withColumn('Row', row_number().over(Window.partitionBy('label').orderBy('Random')))
    )
    
temp_class_count = temp.groupBy('label').count().collect()
class_counts = {row.asDict()['label'] : row.asDict()['count']
                for row in temp_class_count}
labels = [ row.asDict()['label'] for row in temp_class_count]

trainingData = temp.where(reduce(or_,
    (((col('label')== label) & (col('Row') < class_counts[label] * 0.8))
    for label in labels)
    ))
trainingData.cache()

testingData = temp.join(trainingData, on = 'id', how='left_anti')
testingData.cache()

trainingData = trainingData.drop('id', 'Random', 'Row')
testingData = testingData.drop('id', 'Random', 'Row')

print_class_balance(data_binary, 'features')
print_class_balance(trainingData, 'training')
print_class_balance(testingData, 'testing')

#  features
#   420620
#   label   count     ratio
#      1   40666  0.096681
#      0  379954  0.903319

#  training
#   336495
#   label   count     ratio
#      1   32532  0.096679
#      0  303963  0.903321

#   testing
#    84125
#   label  count     ratio
#      1   8134  0.096689
#      0  75991  0.903311

# ---------------------------------------------------------
# Q2 (d)
# Training
from pyspark.ml.classification import LogisticRegression, LinearSVC, RandomForestClassifier

methods = {
    'Logistic Regression': LogisticRegression(),
    'Linear SVC': LinearSVC(),
    'Random Forests': RandomForestClassifier()
    }
LR = LogisticRegression()
SVC = LinearSVC()
RF = RandomForestClassifier()

# ---------------------------------------------------------
# Q2 (e) Performance on testing set
from pyspark.ml.evaluation import BinaryClassificationEvaluator

def print_binary_metrics(predictions, labelCol='label', predictionCol='prediction'):
    
    total = predictions.count()
    positive = predictions.filter((col(labelCol) == 1)).count()
    negative = predictions.filter((col(labelCol) == 0)).count()
    nP = predictions.filter((col(predictionCol) == 1)).count()
    nN = predictions.filter((col(predictionCol) == 0)).count()
    TP = predictions.filter((col(predictionCol) == 1) & (col(labelCol) == 1)).count()
    FP = predictions.filter((col(predictionCol) == 1) & (col(labelCol) == 0)).count()
    TN = predictions.filter((col(predictionCol) == 0) & (col(labelCol) == 0)).count()
    FN = predictions.filter((col(predictionCol) == 0) & (col(labelCol) == 1)).count()  
    
    binary_evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction", 
    labelCol=labelCol, metricName="areaUnderROC")
    auroc = binary_evaluator.evaluate(predictions)
    
    print(f'actual total:    {total}')
    print(f'actual positive: {positive}')
    print(f'actual negative: {negative}')
    print(f'nP:              {nP}')            
    print(f'nN:              {nN}')   
    print(f'TP:              {TP}')  
    print(f'FP:              {FP}')   
    print(f'TN:              {TN}')  
    print(f'FN:              {FN}')
    if TP == 0:
        print('precision:       0')
        print(f'recall:         0')
        print("f1 score:        0")
    else:
        precision = TP/(TP+FP)
        recall = TP/(TP+FN)
        print(f"precision:       {precision}")
        print(f"recall:          {recall}")
        print(f"f1 score:        {2*precision*recall/(precision+recall)}")
    print(f"accuracy:        {(TP+TN)/total}")
    print(f"auroc:           {auroc}")
    
    
def with_custom_prediction(predictions, threshold):
    def apply_custom_threshold(probability, threshold):
        return int(probability[1] > threshold)
    apply_custom_threshold_udf = udf(lambda x: apply_custom_threshold(x, threshold), IntegerType())
    return predictions.withColumn('customPrediction', 
        apply_custom_threshold_udf(col("probability"))
        )
    
# Define a function to train and get the test performance
def bi_clf_performance(name, method, train, test):
    model = method.fit(train)
    predictions = model.transform(test)
    print(f"-----------Performance of {name} on testing set-----------")
    print_binary_metrics(predictions)
    print("-----------------------------------------------------------")   
    
 
# ------------
# No sampling
# ------------
   
# Performance on class imbalanced training set
for name, method in methods.items():
    bi_clf_performance(name, method, trainingData, testingData)

# ------------
# Downsampling
# ------------
trainingData_downsampled = (
    trainingData
    .withColumn('Random', rand())
    .where((col('label')!=0) | ((col('label')==0) & (col('Random') < 6 * (40666 / 379954))))
    .drop("Random")
    )
trainingData_downsampled.cache()
print_class_balance(trainingData_downsampled, "trainingData_downsampled")

# Performance on downsampled training set
for name, method in methods.items():
    bi_clf_performance(name, method, trainingData_downsampled, testingData)

# ------------
# Oversampling
# ------------
import numpy as np
ratio = 6
n = 10
p = ratio / n  # ratio < n such that probability < 1

def random_resample(x, n, p):
    # Can implement custom sampling logic per class,
    if x == 0:
        return [0]  # no sampling
    if x == 1:
        return list(range((np.sum(np.random.random(n) > p))))  # upsampling
    return []  # drop

random_resample_udf = udf(lambda x: random_resample(x, n, p), ArrayType(IntegerType()))

# Oversample the previously undersampled data
trainingData_down_oversampled = (
    trainingData_downsampled
    .withColumn("Sample", random_resample_udf(col("label")))
    .select(
        col("features"),
        col("label"),
        explode(col("Sample")).alias("Sample")
    )
    .drop("Sample")
)
trainingData_down_oversampled.cache()
print_class_balance(trainingData_down_oversampled, "trainingData_down_oversampled")

# Performance on oversampled training set
for name, method in methods.items():
    bi_clf_performance(name, method, trainingData_down_oversampled, testingData)    
    
    
# ------------------------
# Observation reweighting
# ------------------------

trainingData_weighted = (
    trainingData
    .withColumn(
        "Weight",
        when(col("label") == 0, 1.0)
        .when(col("label") == 1, 10.0)
        .otherwise(1.0)
    )
)

weights = (
    trainingData_weighted
    .groupBy("label")
    .agg(
        collect_set(col("Weight")).alias("Weights")
    )
    .toPandas()
)
print(weights)

#    Class Weights
# 0      1  [10.0]
# 1      0   [1.0]

LR_weighted = LogisticRegression(featuresCol='features', labelCol='label', weightCol="Weight")
bi_clf_performance('logistic regression', LR_weighted, trainingData_weighted, testingData)

# Adjust the threshold
LR_weighted_model = LR_weighted.fit(trainingData_weighted)
predictions = LR_weighted_model.transform(testingData)
predictions = with_custom_prediction(predictions, 0.4)
print_binary_metrics(predictions, predictionCol="customPrediction")

# ---------------------------------------------------------
# Q2 (f) Hyperparameter tuned models trained on class balanced data
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

LR_paramGrid = (
    ParamGridBuilder()
    .addGrid(LR.regParam, [0, 0.01, 0.1]) # regularization strength
    .addGrid(LR.elasticNetParam, [0, 1]) # penalty type, 0 for L2, 1 for L1
    .addGrid(LR.threshold, [0.2, 0.5, 0.8])   
    .build()
    )
 
SVC_paramGrid = (
    ParamGridBuilder()
    .addGrid(SVC.regParam, [0, 0.01, 0.1])
    .build()
    )
    
RF_paramGrid = (
    ParamGridBuilder()
    .addGrid(RF.numTrees, [100, 150]) # Number of trees t
    .addGrid(RF.maxDepth, [5, 10]) # Maximum depth of the tree
    .addGrid(RF.maxBins, [16, 32]) # Max number of bins for discretizing continuous features
    .build()
    )
    
methods = {
    'Logistic Regression': (LogisticRegression(), LR_paramGrid),
    'Linear SVC': (LinearSVC(), SVC_paramGrid),
    'Random Forests': (RandomForestClassifier(), RF_paramGrid)
    }
   
def cv_clf_performance(name, method, paramGrid, train, test, evaluator):
    cv = CrossValidator(estimator=method,
                        estimatorParamMaps=paramGrid,
                        evaluator=evaluator,
                        numFolds=5)
    cv_model = cv.fit(train)
    cv_prediction = cv_model.transform(test)
    print(f"-----------Performance of {name} on testing set-----------")
    print_binary_metrics(cv_prediction)
    print("-----------------------------------------------------------")
    
binary_evaluator = BinaryClassificationEvaluator()

# Train on oversampled training set 
for name, (method, paramGrid) in methods.items():
    cv_clf_performance(name, method, paramGrid, trainingData_down_oversampled, testingData, binary_evaluator)


# ---------------------------------------------------------
# ---------------Q3 Multiclass Classification--------------
# ---------------------------------------------------------
# Q3 (b)
# Create label index
from pyspark.ml.feature import StringIndexer
label_idx = StringIndexer(inputCol='GENRE', outputCol='label')
data_multi = label_idx.fit(dataset).transform(dataset)
data_multi.show()

# ---------------------------------------------------------
# Q3 (c) 
# Exact Stratified split using Window
temp = (
    data_multi
    .withColumn('id', monotonically_increasing_id())
    .withColumn('Random', rand())
    .withColumn('Row', row_number().over(Window.partitionBy('label').orderBy('Random')))
    )
    
temp_class_count = temp.groupBy('label').count().collect()
class_counts = {row.asDict()['label'] : row.asDict()['count']
                for row in temp_class_count}
labels = [ row.asDict()['label'] for row in temp_class_count]

train = temp.where(reduce(or_,
    (((col('label')== label) & (col('Row') < class_counts[label] * 0.8))
    for label in labels)
    ))
train.cache()

test = temp.join(train, on = 'id', how='left_anti')
test.cache()

train = train.drop('id', 'Random', 'Row')
test = test.drop('id', 'Random', 'Row')

print_class_balance(data_multi, 'features')
print_class_balance(train, 'training')
print_class_balance(test, 'testing')

# Metrics
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.mllib.evaluation import MulticlassMetrics

multi_evaluator = MulticlassClassificationEvaluator()

# Model without hyperparameter tuning
def multi_clf_performance(name, method, train, test):
    model = method.fit(train)
    prediction = model.transform(test)
    print(f"-----------Performance of {name} on testing set-----------")
    # Compute raw scores on the test set
    predictionAndLabels = prediction.select('prediction', 'label')
    # Instantiate metrics object
    metrics = MulticlassMetrics(predictionAndLabels.rdd)
    # Overall statistics
    print("----------Summary Stats----------------------")
    print(f"Weighted precision: {multi_evaluator.evaluate(prediction, {multi_evaluator.metricName: 'weightedPrecision'})}")
    print(f"Weighted recall: {multi_evaluator.evaluate(prediction, {multi_evaluator.metricName: 'weightedRecall'})}")
    print(f"F1 Score: {multi_evaluator.evaluate(prediction, {multi_evaluator.metricName: 'f1'})}")
    print(f"Accuracy: {multi_evaluator.evaluate(prediction, {multi_evaluator.metricName: 'accuracy'})}")

    # Statistics by class
    print("--------Stats by class----------------------")
    labels = [row.asDict()['label'] for row in test.select('label').distinct().collect()]
    for label in sorted(labels):
        print("Class %s precision = %s" % (label, metrics.precision(label)))
        print("Class %s recall = %s" % (label, metrics.recall(label)))
        print("Class %s F1 Score = %s" % (label, metrics.fMeasure(label, beta=1.0)))

    # Weighted stats
    #print("--------Weighted Stats----------------------")
    #print("Weighted precision = %s" % metrics.weightedPrecision)
    #print("Weighted recall = %s" % metrics.weightedRecall) 
    #print("Weighted F1 Score = %s" % metrics.weightedFMeasure())
    #print("Weighted false positive rate = %s" % metrics.weightedFalsePositiveRate)
    print("-----------------------------------------------------------")

RF = RandomForestClassifier()
multi_clf_performance('random forests', RF, train, test)
LR = LogisticRegression()
multi_clf_performance('logistic regression', LR, train, test)

# Model with CV hyperparameter tuning
def cv_multi_clf_performance(name, method, paramGrid, train, test):
    cv = CrossValidator(estimator=method,
                        estimatorParamMaps=paramGrid,
                        evaluator=multi_evaluator,
                        numFolds=5)
    multi_clf_performance(name, cv, train, test)
    
    
 RF_paramGrid = (
    ParamGridBuilder()
    .addGrid(RF.numTrees, [50, 100, 150]) # Number of trees t
    .addGrid(RF.maxDepth, [10, 15, 20]) # Maximum depth of the tree
    .addGrid(RF.maxBins, [16, 32]) # Max number of bins for discretizing continuous features
    .build()
    )
    
LR_paramGrid = (
    ParamGridBuilder()
    .addGrid(LR.regParam, [0, 0.01, 0.1]) # regularization strength
    .addGrid(LR.elasticNetParam, [0, 1]) # penalty type, 0 for L2, 1 for L1
    .addGrid(LR.threshold, [0.3, 0.5, 0.7])   
    .build()
    )    
 
cv_multi_clf_performance('random forests', RF, RF_paramGrid, train, test)

cv_multi_clf_performance('logistic regression', LR, LR_paramGrid, train, test)



