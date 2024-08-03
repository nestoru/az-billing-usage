import json
import subprocess
import argparse

# Set up argument parsing
parser = argparse.ArgumentParser(description='Fetch Azure consumption usage details.')
parser.add_argument('subscription_id', type=str, help='Azure Subscription ID')
parser.add_argument('start_date', type=str, help='Start date in YYYY-MM-DD format')
parser.add_argument('end_date', type=str, help='End date in YYYY-MM-DD format')

args = parser.parse_args()

subscription_id = args.subscription_id
start_date = args.start_date
end_date = args.end_date
api_version = "2019-11-01"

url = f"https://management.azure.com/subscriptions/{subscription_id}/providers/Microsoft.Consumption/usageDetails?startDate={start_date}&endDate={end_date}&api-version={api_version}"
results = []

while url:
    command = ['az', 'rest', '--method', 'GET', '--url', url, '--output', 'json']
    # print(f"Executing command: {' '.join(command)}")  # Print the command being executed
    
    # Execute the az rest command and capture the output
    result = subprocess.run(command, capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"Error: {result.stderr}")
        break
    
    # Parse the JSON response
    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError as e:
        print(f"JSON decode error: {e}")
        break
    
    if 'error' in data:
        print(f"API error: {data['error']['message']}")
        break
    
    # Add the current page of results to the list
    results.extend(data.get('value', []))
    
    # Get the nextLink for pagination, if present
    url = data.get('nextLink')

# Print the combined results as JSON
print(json.dumps({"value": results}, indent=2))

