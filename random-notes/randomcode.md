Here's how you can proceed to build a **training model** using the features generated from the above code (after joining and removing duplicates) in PySpark. For this example, we will use **Logistic Regression** as the model, but you can choose other models depending on your use case.

### Step-by-Step Workflow for Training a Model:

1. **Prepare the Data**: Use the `final_features_df` created in the earlier steps as the input for training.
2. **Split the Data**: Split the data into training and testing datasets.
3. **Train a Model**: Train a Logistic Regression model using PySpark's MLlib.
4. **Evaluate the Model**: Measure model performance using accuracy, precision, or other metrics.
5. **Save the Model**: Optionally log the model with **MLflow**.

### Example PySpark Code for Model Training

#### 1. **Prepare the Data for Training**

Assume that `final_features_df` is the DataFrame generated after cleaning and joining the data.

- The `target_column` is the column that you want to predict (the label), which should already be present in the DataFrame.
- The `feature_columns` are the columns you want to use for training.

#### Example:

```python
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Define the feature columns and label column
feature_columns = ['total_lost_work_days', 'performance_score', 'total_medical_cost']  # Replace with actual feature names
label_column = 'target_column'  # Replace with actual target column (the one you're predicting)

# Create a VectorAssembler to combine feature columns into a single vector
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")

# Apply the assembler transformation to prepare the dataset
final_features_vector_df = assembler.transform(final_features_df)

# Select only the features and label for model training
training_data = final_features_vector_df.select("features", label_column)
```

#### 2. **Split the Data into Training and Test Sets**

```python
# Split the data into training (70%) and testing (30%)
train_data, test_data = training_data.randomSplit([0.7, 0.3], seed=42)
```

#### 3. **Train a Logistic Regression Model**

You can use **Logistic Regression** for classification or choose other models like Random Forest, Decision Trees, or Gradient Boosting depending on your needs.

```python
# Initialize Logistic Regression model
logistic_regression = LogisticRegression(labelCol=label_column, featuresCol="features")

# Train the model
lr_model = logistic_regression.fit(train_data)
```

#### 4. **Evaluate the Model**

Evaluate the model’s performance on the test dataset:

```python
# Make predictions on the test set
predictions = lr_model.transform(test_data)

# Initialize the evaluator
evaluator = MulticlassClassificationEvaluator(labelCol=label_column, predictionCol="prediction", metricName="accuracy")

# Calculate accuracy
accuracy = evaluator.evaluate(predictions)
print(f"Model Accuracy: {accuracy}")
```

You can also compute other metrics like precision, recall, or F1-score by changing the `metricName` in the `MulticlassClassificationEvaluator`.

#### 5. **Log the Model with MLflow (Optional)**

You can log your model, metrics, and parameters using **MLflow** for future use and deployment.

```python
import mlflow
import mlflow.spark

# Log the model and metrics using MLflow
with mlflow.start_run():
    mlflow.log_metric("accuracy", accuracy)
    mlflow.spark.log_model(lr_model, "logistic_regression_model")
```

#### 6. **Save the Model (Optional)**

You can save the model locally or in a cloud location for later use:

```python
# Save the trained model
lr_model.write().overwrite().save("/path/to/save/logistic_regression_model")
```

### Summary of Steps:

1. **Feature Preparation**: We combine the selected feature columns into a feature vector using `VectorAssembler`.
2. **Model Training**: We split the data and train a **Logistic Regression** model.
3. **Evaluation**: We evaluate the model using metrics like accuracy and log it to **MLflow**.
4. **Model Saving**: The trained model can be saved for future use or deployment.

This template can be extended to other types of models (e.g., RandomForest, GradientBoostedTrees, etc.), depending on your use case.

Would you like to try a different model or specific customization for feature selection? Let me know!



To validate the trained model using **MLflow**, you can follow these steps:

1. **Train the model**: As described previously, the model is trained and logged using MLflow.
2. **Register the model in MLflow**: Once the model is logged, you register it in the **MLflow Model Registry**.
3. **Load the model from MLflow**: You can load the model from the registry for validation.
4. **Validate the model**: You validate the model using the test dataset by comparing the predictions with the actual values.
5. **Log validation metrics**: Log validation metrics (such as accuracy, precision, recall, etc.) back to MLflow for comparison.

Here’s how to implement this end-to-end validation using MLflow.

### Code for Model Validation with MLflow

#### 1. **Train and Log the Model**

This assumes you have already trained a model using the steps outlined previously.

```python
import mlflow
import mlflow.spark
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Define feature columns and label column
feature_columns = ['total_lost_work_days', 'performance_score', 'total_medical_cost']  # Replace with your actual columns
label_column = 'target_column'  # Replace with the actual target column

# Assemble feature vector
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
final_features_vector_df = assembler.transform(final_features_df)

# Select features and label
training_data = final_features_vector_df.select("features", label_column)

# Split the data into training and test sets
train_data, test_data = training_data.randomSplit([0.7, 0.3], seed=42)

# Train the logistic regression model
logistic_regression = LogisticRegression(labelCol=label_column, featuresCol="features")
lr_model = logistic_regression.fit(train_data)

# Log the model and metrics in MLflow
with mlflow.start_run() as run:
    # Log metrics (Accuracy, etc.)
    predictions = lr_model.transform(test_data)
    evaluator = MulticlassClassificationEvaluator(labelCol=label_column, predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)
    mlflow.log_metric("accuracy", accuracy)
    
    # Log the model
    mlflow.spark.log_model(lr_model, "logistic_regression_model")
    
    # Register the model (optional, if you want to use the MLflow Model Registry)
    model_uri = f"runs:/{run.info.run_id}/logistic_regression_model"
    mlflow.register_model(model_uri, "client_a_model")
```

#### 2. **Load the Model from MLflow**

Once the model is registered and saved in MLflow, you can load it back for validation. This is useful when you're validating the model in a different environment or at a later time.

```python
# Load the registered model from MLflow Model Registry
model_name = "client_a_model"
model_version = 1  # Replace with the correct version if needed

# Load the model using MLflow
loaded_model = mlflow.spark.load_model(f"models:/{model_name}/{model_version}")
```

#### 3. **Validate the Model on the Test Data**

Now, use the loaded model to make predictions and validate it.

```python
# Use the loaded model to make predictions on the test data
predictions = loaded_model.transform(test_data)

# Evaluate the model's accuracy
evaluator = MulticlassClassificationEvaluator(labelCol=label_column, predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print(f"Validation Accuracy: {accuracy}")
```

#### 4. **Log Validation Metrics Back to MLflow**

You can log the validation metrics (accuracy, precision, recall, etc.) back to MLflow for comparison across models.

```python
with mlflow.start_run() as validation_run:
    # Log validation metrics
    mlflow.log_metric("validation_accuracy", accuracy)
    
    # Optionally log other metrics like precision, recall, or confusion matrix
    precision_evaluator = MulticlassClassificationEvaluator(labelCol=label_column, predictionCol="prediction", metricName="weightedPrecision")
    precision = precision_evaluator.evaluate(predictions)
    mlflow.log_metric("validation_precision", precision)

    recall_evaluator = MulticlassClassificationEvaluator(labelCol=label_column, predictionCol="prediction", metricName="weightedRecall")
    recall = recall_evaluator.evaluate(predictions)
    mlflow.log_metric("validation_recall", recall)

    # Save the validation run for future reference
    print(f"Validation run ID: {validation_run.info.run_id}")
```

### Summary of Steps:

1. **Model Training**:
   - Train the model and log it to MLflow.
   - Optionally register the model in the MLflow Model Registry.

2. **Model Validation**:
   - Load the model from MLflow.
   - Validate it by predicting on the test set.
   - Log validation metrics (accuracy, precision, recall) back to MLflow for comparison.

### MLflow UI:
Once this is done, you can track and compare all your experiments and validation runs directly in the MLflow UI.

- **Run Comparison**: You can compare different runs to see which version of the model performed best.
- **Model Tracking**: Use the Model Registry to manage and deploy models as needed.

Would you like more detailed instructions on deploying or comparing models in MLflow? Let me know!
