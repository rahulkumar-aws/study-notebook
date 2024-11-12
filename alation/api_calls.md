```python
import requests

# Replace these with your actual details
alation_instance = "https://your_alation_instance.com"  # Your Alation instance URL
api_key = "your_api_key"  # Your API key
asset_type = "database"  # Replace with the correct asset type, e.g., "database", "schema", "table"
asset_id = 306  # Replace with the specific asset ID you want to retrieve

# Construct the API endpoint URL
url = f"{alation_instance}/api/v2/catalog/{asset_type}/{asset_id}/"

# Set up the headers with the API key for authorization
headers = {
    "Authorization": f"Token {api_key}",
    "Content-Type": "application/json"
}

# Make the GET request
response = requests.get(url, headers=headers)

# Check if the request was successful
if response.status_code == 200:
    data = response.json()  # Parse the JSON response
    print("Asset Information:")
    print(data)
else:
    print(f"Failed to retrieve asset information. Status code: {response.status_code}")
    print("Response:", response.text)
```
