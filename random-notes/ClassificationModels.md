Here is the **full PySpark code** to create and validate models for **classification targets** like `eligibility_status` or `risk_level`. The code includes the steps for training both **Logistic Regression** and **Random Forest Classifier**, evaluating the models, and logging the results to **MLflow**.

### Full PySpark Code for Classification Models

```python
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import mlflow
import mlflow.spark

# Assume final_features_df is already loaded from Feature Store or catalog

# Define the feature columns and target column
feature_columns = ['total_lost_work_days', 'performance_score', 'total_medical_cost']  # Replace with actual feature columns
label_column = 'eligibility_status'  # Replace with the actual target column (classification target)

# Prepare the feature vector for modeling
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
final_features_vector_df = assembler.transform(final_features_df)

# Select the features and label for classification
classification_data = final_features_vector_df.select("features", label_column)

# Split the data into training and test datasets (70% training, 30% testing)
train_data, test_data = classification_data.randomSplit([0.7, 0.3], seed=42)

### Logistic Regression Model ###
# Initialize Logistic Regression model
logistic_regression = LogisticRegression(labelCol=label_column, featuresCol="features")

# Train the Logistic Regression model
lr_model = logistic_regression.fit(train_data)

# Make predictions on the test data
lr_predictions = lr_model.transform(test_data)

# Evaluate the Logistic Regression model
lr_evaluator = MulticlassClassificationEvaluator(labelCol=label_column, predictionCol="prediction", metricName="accuracy")
lr_accuracy = lr_evaluator.evaluate(lr_predictions)
print(f"Logistic Regression Model Accuracy for {label_column}: {lr_accuracy}")

### Random Forest Classifier Model ###
# Initialize Random Forest Classifier model
random_forest_classifier = RandomForestClassifier(labelCol=label_column, featuresCol="features")

# Train the Random Forest Classifier model
rf_model = random_forest_classifier.fit(train_data)

# Make predictions on the test data
rf_predictions = rf_model.transform(test_data)

# Evaluate the Random Forest Classifier model
rf_accuracy = lr_evaluator.evaluate(rf_predictions)
print(f"Random Forest Model Accuracy for {label_column}: {rf_accuracy}")

### Log the Models and Metrics using MLflow ###
with mlflow.start_run() as run:
    # Log the accuracy for Logistic Regression
    mlflow.log_metric("logistic_regression_accuracy", lr_accuracy)
    
    # Log the Logistic Regression model
    mlflow.spark.log_model(lr_model, "logistic_regression_model")
    
    # Log the accuracy for Random Forest
    mlflow.log_metric("random_forest_accuracy", rf_accuracy)
    
    # Log the Random Forest model
    mlflow.spark.log_model(rf_model, "random_forest_model")

    print(f"Model run ID: {run.info.run_id}")
```

### Key Components:

1. **Feature Preparation**:
   - We use `VectorAssembler` to combine the feature columns into a vector called `features`.
   
2. **Data Splitting**:
   - The data is split into training and test datasets using a 70/30 split.

3. **Logistic Regression Model**:
   - The Logistic Regression model is initialized, trained, and evaluated using **accuracy** as the metric.

4. **Random Forest Classifier Model**:
   - Similarly, a Random Forest Classifier model is trained and evaluated using the same accuracy metric.

5. **MLflow Logging**:
   - Both the **accuracy** of the models and the trained models themselves are logged into **MLflow** for tracking.
   - You can later retrieve and deploy these models from the MLflow UI or API.

### How to Use This Code:
1. **Feature Columns**: Replace the `feature_columns` list with the actual column names from your dataset.
2. **Target Column**: Set the `label_column` to the actual target column (e.g., `eligibility_status`, `risk_level`).
3. **MLflow**: Run the code in a Databricks notebook or an environment with **MLflow** enabled. You can monitor the results in the **MLflow UI**.

### Next Steps:
Once this model is trained and logged, you can:
- **Validate the models** by loading them from MLflow and running them on new data.
- **Deploy the models** to serve real-time or batch predictions.

Would you like assistance with the validation or deployment of these models? Let me know if you need further help!
