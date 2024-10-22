from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Assuming final_features_df is already loaded and contains the required feature columns
# Define the feature columns and target column
feature_columns = ['total_lost_work_days', 'performance_score', 'total_medical_cost']  # Replace with actual feature columns
label_column = 'eligibility_status'  # Replace with your actual target column

# Step 1: Prepare the feature vector for modeling
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
final_features_vector_df = assembler.transform(final_features_df)

# Select the features and label for classification
classification_data = final_features_vector_df.select("features", label_column)

# Step 2: Split the data into training and testing sets (70% training, 30% testing)
train_data, test_data = classification_data.randomSplit([0.7, 0.3], seed=42)

### Step 3: Logistic Regression Model ###
# Initialize Logistic Regression model
logistic_regression = LogisticRegression(labelCol=label_column, featuresCol="features")

# Train the Logistic Regression model
lr_model = logistic_regression.fit(train_data)

# Make predictions on the test data
lr_predictions = lr_model.transform(test_data)

# Evaluate the Logistic Regression model using accuracy
lr_evaluator = MulticlassClassificationEvaluator(labelCol=label_column, predictionCol="prediction", metricName="accuracy")
lr_accuracy = lr_evaluator.evaluate(lr_predictions)
print(f"Logistic Regression Model Accuracy for {label_column}: {lr_accuracy}")

### Step 4: Random Forest Classifier Model ###
# Initialize Random Forest Classifier model
random_forest_classifier = RandomForestClassifier(labelCol=label_column, featuresCol="features")

# Train the Random Forest Classifier model
rf_model = random_forest_classifier.fit(train_data)

# Make predictions on the test data
rf_predictions = rf_model.transform(test_data)

# Evaluate the Random Forest Classifier model using accuracy
rf_accuracy = lr_evaluator.evaluate(rf_predictions)
print(f"Random Forest Model Accuracy for {label_column}: {rf_accuracy}")
