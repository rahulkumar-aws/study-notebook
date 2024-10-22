import mlflow
import pandas as pd

# Load the Random Forest model from the Databricks Model Registry
def load_and_use_model():
    # Load the model from the registry (use "latest" or specify a version)
    model_name = "my_random_forest_model"
    model_version = "latest"  # Or specify a version, e.g., "1"
    
    # Load the registered model
    model = mlflow.pyfunc.load_model(f"models:/{model_name}/{model_version}")
    
    # Example: Load new data for inference (use your real data here)
    # Replace with your actual feature columns
    X_test = pd.DataFrame({
        "Leave_Type": [1, 0, 1],
        "Unique_Member_Count": [1, 1, 1],
        "LTD_Claim_Counter": [0, 0, 0],
        "WC_any_Claim_Counter": [0, 0, 1],
        "STD_Claim_Counter": [1, 1, 1],
        "tenure": [29.88, 5.92, 18.25],
        "final_AnnualSalary": [106092, 139300, 115700],
        "CHRON_ALLERGIES_IND": [0, 0, 0],
        "CHRON_ARTHRITIS_IND": [0, 1, 0],
        "CHRON_ASTHMA_IND": [0, 0, 0],
        "total_medical_cost": [107.03, 71.85, 8341.23]
    })

    # Make predictions using the loaded model
    predictions = model.predict(X_test)

    # Output the predictions
    print("Predictions:", predictions)

if __name__ == "__main__":
    load_and_use_model()
