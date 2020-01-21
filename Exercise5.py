###### DATA420 Exercise5
##### San Francisco Crime Classification
start_pyspark_shell -e 4 -c 2 -w 4 -m 4

from pyspark.sql import SQLContext
from pyspark import SparkContext
sc = SparkContext()
sqlContext = SQLContext(sc)

# Loading a CSV file
data = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('hdfs:///user/psh99/crime/train.csv')

# Remove the columns we do not need and have a look the first five rows
drop_list = ['Dates', 'DayOfWeek', 'PdDistrict', 'Resolution', 'Address', 'X', 'Y']
data = data.select([column for column in data.columns if column not in drop_list])
data.show(5)

# Apply printSchema() on the data which will print the schema in a tree format
data.printSchema()

# Top 20 crime categories
from pyspark.sql.functions import col
data.groupBy("Category") \
    .count() \
    .orderBy(col("count").desc()) \
    .show()

# Top 20 crime descriptions
data.groupBy("Descript") \
    .count() \
    .orderBy(col("count").desc()) \
    .show()

# NLP model pipeline	
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer
from pyspark.ml.classification import LogisticRegression
# regular expression tokenizer
regexTokenizer = RegexTokenizer(inputCol="Descript", outputCol="words", pattern="\\W")
# stop words
add_stopwords = ["http","https","amp","rt","t","c","the"]
stopwordsRemover = StopWordsRemover(inputCol="words", outputCol="filtered").setStopWords(add_stopwords)
# bag of words count
countVectors = CountVectorizer(inputCol="filtered", outputCol="features", vocabSize=10000, minDF=5)

# The label column (Category) will be encoded to label indices, from 0 to 32;
# the most frequent label (LARCENY/THEFT) will be indexed as 0.
from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
label_stringIdx = StringIndexer(inputCol = "Category", outputCol = "label")
pipeline = Pipeline(stages=[regexTokenizer, stopwordsRemover, countVectors, label_stringIdx])
# Fit the pipeline to training documents.
pipelineFit = pipeline.fit(data)
dataset = pipelineFit.transform(data)
dataset.show(5)

# Partition Training & Test sets
# Randomly split data into training and testing sets, set seed for reproducibility.
(trainingData, testData) = dataset.randomSplit([0.7, 0.3], seed = 100)
print("Training Dataset Count: " + str(trainingData.count()))    # Training Dataset Count: 614666
print("Test Dataset Count: " + str(testData.count()))  # Test Dataset Count: 263383


# Model Training and Evaluation

# 1.Logistic Regression using Count Vector Features
lr = LogisticRegression(maxIter=20, regParam=0.3, elasticNetParam=0)
lrModel = lr.fit(trainingData)
predictions = lrModel.transform(testData)
predictions.filter(predictions['prediction'] == 0) \
    .select("Descript","Category","probability","label","prediction") \
    .orderBy("probability", ascending=False) \
    .show(n = 10, truncate = 30)
# Evaluate the performance
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
evaluator.evaluate(predictions) # 0.9725282146509521

# 2.Logistic Regression using TF-IDF Features
from pyspark.ml.feature import HashingTF, IDF
# Add HashingTF and IDF to transformation
hashingTF = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=10000)
idf = IDF(inputCol="rawFeatures", outputCol="features", minDocFreq=5) #minDocFreq: remove sparse terms
# Redo Pipeline
pipeline = Pipeline(stages=[regexTokenizer, stopwordsRemover, hashingTF, idf, label_stringIdx])
pipelineFit = pipeline.fit(data)
dataset = pipelineFit.transform(data)
# Randomly split data into training and test sets. set seed for reproducibility
(trainingData, testData) = dataset.randomSplit([0.7, 0.3], seed = 100)
# Build the model
lr = LogisticRegression(maxIter=20, regParam=0.3, elasticNetParam=0)
# Train model with Training Data
lrModel = lr.fit(trainingData)
# Make predictions on Testing Data
predictions = lrModel.transform(testData)
predictions.filter(predictions['prediction'] == 0) \
    .select("Descript","Category","probability","label","prediction") \
    .orderBy("probability", ascending=False) \
    .show(n = 10, truncate = 30)
# Evaluate the performance
evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
evaluator.evaluate(predictions) # 0.9722954523853476

# 3. Using cross-validation to tune our hyper parameters, and we will only tune the count vectors Logistic Regression
pipeline = Pipeline(stages=[regexTokenizer, stopwordsRemover, countVectors, label_stringIdx])
pipelineFit = pipeline.fit(data)
dataset = pipelineFit.transform(data)
(trainingData, testData) = dataset.randomSplit([0.7, 0.3], seed = 100)
# Build the model
lr = LogisticRegression(maxIter=20, regParam=0.3, elasticNetParam=0)
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
# Create ParamGrid for Cross Validation
paramGrid = (ParamGridBuilder()
             .addGrid(lr.regParam, [0.1, 0.3, 0.5]) # regularization parameter
             .addGrid(lr.elasticNetParam, [0.0, 0.1, 0.2]) # Elastic Net Parameter (Ridge = 0)
#            .addGrid(model.maxIter, [10, 20, 50]) #Number of iterations
#            .addGrid(idf.numFeatures, [10, 100, 1000]) # Number of features
             .build())
# Create 5-fold CrossValidator
cv = CrossValidator(estimator=lr, \
                    estimatorParamMaps=paramGrid, \
                    evaluator=evaluator, \
                    numFolds=5)
# Run cross validations
cvModel = cv.fit(trainingData)  # this will likely take a fair amount of time because of the amount of models that we're
                                # creating and testing
# Use test set here so we can measure the accuracy of our model on new data
predictions = cvModel.transform(testData) # cvModel uses the best model found from the Cross Validation
# Evaluate best model
evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
evaluator.evaluate(predictions) # 0.9919124901837848


# 4.Naive Bayes

from pyspark.ml.classification import NaiveBayes
# create the trainer and set its parameters
nb = NaiveBayes(smoothing=1)
# train the model
model = nb.fit(trainingData)
# Make predictions on Testing Data
predictions = model.transform(testData)
predictions.filter(predictions['prediction'] == 0) \
    .select("Descript","Category","probability","label","prediction") \
    .orderBy("probability", ascending=False) \
    .show(n = 10, truncate = 30)
# Evaluate the performance
evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
evaluator.evaluate(predictions) # 0.9939847228864086


# 5.Random forest

from pyspark.ml.classification import RandomForestClassifier
# Create an initial RandomForest model
rf = RandomForestClassifier(labelCol="label", \
                            featuresCol="features", \
                            numTrees = 100, \
                            maxDepth = 4, \
                            maxBins = 32)
# Train model with Training Data
rfModel = rf.fit(trainingData)
# Make predictions on Testing Data
predictions = rfModel.transform(testData)
predictions.filter(predictions['prediction'] == 0) \
    .select("Descript","Category","probability","label","prediction") \
    .orderBy("probability", ascending=False) \
    .show(n = 10, truncate = 30)
# Evaluate the performance
evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
evaluator.evaluate(predictions) # 0.6678726526735538


# Using cross-validation
# Create ParamGrid for Cross Validation
paramGrid = (ParamGridBuilder()
             .addGrid(rf.numTrees, [50, 100, 200]) # number of trees
             .addGrid(rf.maxDepth, [3, 4, 5]) # maximum depth
#            .addGrid(rf.maxBins, [24, 32, 40]) #Number of bins
             .build())
# Create 5-fold CrossValidator
cv = CrossValidator(estimator=rf, \
                    estimatorParamMaps=paramGrid, \
                    evaluator=evaluator, \
                    numFolds=5)
# Run cross validations
cvModel = cv.fit(trainingData)
# Use test set here so we can measure the accuracy of our model on new data
predictions = cvModel.transform(testData)
# Evaluate the performance
evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
evaluator.evaluate(predictions) # 0.7200303989547759
