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

### Grand total
Using price multiplied by quantity:
```
cat ~/Downloads/az_usage_details_2024-07-01_2024-07-31.json | jq '[.value[] | .properties | .effectivePrice * .quantity] | add'
```
Using the cost in billing currency. Both answers must match:
```
cat ~/Downloads/az_usage_details_2024-07-01_2024-07-31.json | jq '[.value[] | .properties.costInBillingCurrency] | add'
```

### Total per instance name and grand total
```
jq '
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
### Total per instance sorted by price desc
```
jq '.value | group_by(.properties.instanceName) | map({instance: .[0].properties.instanceName, resourceGroup: .[0].properties.resourceGroup, total_cost: (map(.properties.costInBillingCurrency) | add)}) | sort_by(.total_cost) | reverse'
```

### Total per instance name containing a case insensitive string value
```
jq '[.value[] | select(.properties.instanceName | test("ETL"; "i")) | .properties.costInBillingCurrency] | add'
```

### List the VM names only
```
jq '[.value[]
    | select(.properties.instanceName | test("/virtualMachines/"; "i"))
    | .properties.instanceName
    | split("/") | last
    | ascii_downcase]
    | sort
    | unique
    | .[]' /Users/nu/Downloads/az_usage_details_2024-12-01_2024-12-31.json
```

### Cost per day for an instance name containing a case insensitive string value
```
jq -r '
  [.value[] | select(.properties.instanceName | test("tsavdshare"; "i"))]
  | group_by(.properties.date)
  | map({date: .[0].properties.date, total: (map(.properties.costInBillingCurrency) | add)})
  | sort_by(.date)
  | .[] | "\(.date): $\(.total)"
'
=======
### Total for a resource pattern
```
cat ~/Downloads/az_usage_details_2024-07-01_2024-07-31.json | jq '
.value | 
map(select(.properties.instanceName | contains("part-of-resource-name-here"))) |
map(.properties.effectivePrice * .properties.quantity) | 
add'
```

## Additional handy scripts
```
jq '[.value[] | select(.properties.instanceName | test("ETL"; "i")) | .properties.costInBillingCurrency] | add'
```

### List the VM names only
```
jq '[.value[]
    | select(.properties.instanceName | test("/virtualMachines/"; "i"))
    | .properties.instanceName
    | split("/") | last
    | ascii_downcase]
    | sort
    | unique
    | .[]' /Users/nu/Downloads/az_usage_details_2024-12-01_2024-12-31.json
```

### Cost per day for an instance name containing a case insensitive string value
```
jq -r '
  [.value[] | select(.properties.instanceName | test("tsavdshare"; "i"))]
  | group_by(.properties.date)
  | map({date: .[0].properties.date, total: (map(.properties.costInBillingCurrency) | add)})
  | sort_by(.date)
  | .[] | "\(.date): $\(.total)"
'
### Total for a resource pattern
```
cat ~/Downloads/az_usage_details_2024-07-01_2024-07-31.json | jq '
.value | 
map(select(.properties.instanceName | contains("part-of-resource-name-here"))) |
map(.properties.effectivePrice * .properties.quantity) | 
add'
```

## Additional handy scripts
```
	compare_storage_usage.py
	compare_usage_details.py
	compare_vm_usage.py
	reservations_report.py
```
