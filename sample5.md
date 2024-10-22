import pandas as pd
import mlflow
import mlflow.sklearn
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, precision_score, recall_score
from sklearn.preprocessing import LabelEncoder
from sklearn.impute import SimpleImputer
import databricks.feature_store as feature_store

# Main function for training and logging the model
def main():
    # Step 1: Load data from Databricks Feature Store
    fs = feature_store.FeatureStoreClient()
    final_features_df = fs.read_table(name="acia_hackathon_2024.actuarydb_wizards.client_a_features")

    # Convert PySpark DataFrame to pandas DataFrame
    df = final_features_df.toPandas()

    # Step 2: Data preprocessing
    # Encode categorical columns with LabelEncoder
    label_encoder = LabelEncoder()

    # Encode 'Leave_Type' and 'performance_score'
    df['Leave_Type'] = label_encoder.fit_transform(df['Leave_Type'])
    df['performance_score'] = label_encoder.fit_transform(df['performance_score'])

    # Define the features and the target variable
    feature_columns = [
        'Leave_Type', 'Unique_Member_Count', 'LTD_Claim_Counter', 'WC_any_Claim_Counter', 'STD_Claim_Counter',
        'tenure', 'final_AnnualSalary', 'CHRON_ALLERGIES_IND', 'CHRON_ARTHRITIS_IND', 'CHRON_ASTHMA_IND', 'total_medical_cost'
    ]
    target_column = 'total_lost_work_days'

    # Step 3: Handle missing values in features and target
    # Remove rows where the target variable (y) is NaN
    df = df.dropna(subset=[target_column])

    # Use SimpleImputer to fill missing values in the features (mean strategy for numerical columns)
    imputer = SimpleImputer(strategy='mean')

    # Prepare features (X) and target (y)
    X = df[feature_columns]  # Features
    y = df[target_column]    # Target

    # Impute missing values in the features
    X_imputed = imputer.fit_transform(X)

    # Step 4: Train-test split
    X_train, X_test, y_train, y_test = train_test_split(X_imputed, y, test_size=0.3, random_state=42)

    # Step 5: Train a Random Forest Classifier
    rf_clf = RandomForestClassifier(n_estimators=100, random_state=42)
    rf_clf.fit(X_train, y_train)

    # Make predictions
    y_pred = rf_clf.predict(X_test)

    # Step 6: Log model and metrics to MLflow
    with mlflow.start_run():
        # Log the model to MLflow
        mlflow.sklearn.log_model(rf_clf, "random_forest_model")

        # Calculate metrics
        accuracy = accuracy_score(y_test, y_pred)
        precision = precision_score(y_test, y_pred, average='weighted', zero_division=1)
        recall = recall_score(y_test, y_pred, average='weighted', zero_division=1)

        # Log metrics to MLflow
        mlflow.log_metric("accuracy", accuracy)
        mlflow.log_metric("precision", precision)
        mlflow.log_metric("recall", recall)

        # Print metrics
        print(f"Random Forest Classifier Accuracy: {accuracy}")
        print(f"Random Forest Classifier Precision: {precision}")
        print(f"Random Forest Classifier Recall: {recall}")

        # Log model parameters
        mlflow.log_param("n_estimators", 100)
        mlflow.log_param("random_state", 42)

# Entry point for script execution
if __name__ == "__main__":
    main()
