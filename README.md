# Azure Billing
Extract Azure billing information.

## Preconditions
- You must be a billing admin.
- Login to azure before running the command
```
az login
```

## Extracting raw data
- The command takes some time. You might want to filter the response in a number of different ways. Better to store the query results in a file, for example:
```
python fetch_usage_details.py <your sunscription id here> 2024-07-01 2024-07-31 > ~/Downloads/az_usage_details_2024-07-01_2024-07-31.json
```

## Filtering results
Use jq command to perform all kind of querying on the data. Here are some examples.

### Grand total for July 2024
```
cat ~/Downloads/az_usage_details_2024-07-01_2024-07-31.json | jq '[.value[] | .properties | .effectivePrice * .quantity] | add'
```

### Total per instance name and grand total for July 2024
```
cat ~/Downloads/az_usage_details_2024-07-01_2024-07-31.json | jq '
.value |
group_by(.properties.instanceName | split("/") | last) |
map({
    instance: .[0].properties.instanceName | split("/") | last,
    totalCost: map(.properties.effectivePrice * .properties.quantity) | add
}) |
sort_by(-.totalCost) |
. as $instances |
{
    instances: $instances,
    grandTotal: ($instances | map(.totalCost) | add)
}'
```

