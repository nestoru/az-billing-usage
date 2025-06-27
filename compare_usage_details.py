import os
import re
import sys
import json
import pandas as pd

def load_usage_details(directory, file_pattern):
    """
    Searches for files matching the regex pattern in the given directory,
    loads their contents, and extracts relevant data.
    """
    pattern = re.compile(file_pattern)
    matching_files = [
        os.path.join(directory, f) for f in os.listdir(directory) if pattern.match(f)
    ]

    if not matching_files:
        print("No files matching the pattern were found.")
        sys.exit(1)

    print(f"Found {len(matching_files)} matching files:")
    for f in matching_files:
        print(f"  - {f}")

    all_data = []
    total_records = 0

    for file_path in matching_files:
        print(f"Processing file: {file_path}")
        with open(file_path, 'r', encoding='utf-8', errors='replace') as file:
            try:
                data = json.load(file)
                records_in_file = len(data.get('value', []))
                total_records += records_in_file
                file_total_cost = 0
                
                for record in data.get('value', []):
                    properties = record.get('properties', {})
                    instance_name = properties.get('instanceName', '')
                    resource_name = instance_name.split('/')[-1] if instance_name else ''

                    # Extract correct billing period dates
                    billing_start = properties.get('billingPeriodStartDate', '')
                    billing_end = properties.get('billingPeriodEndDate', '')

                    # Fix invalid billing period dates (0001-01-01 issue)
                    if billing_start.startswith("0001"):
                        billing_start = properties.get('date', '')  # Fallback to actual usage date
                    if billing_end.startswith("0001"):
                        billing_end = properties.get('date', '')  # Fallback to actual usage date

                    calculated_cost = properties.get('quantity', 0) * properties.get('effectivePrice', 0)
                    file_total_cost += calculated_cost

                    all_data.append({
                        'billing_period_start': billing_start,
                        'billing_period_end': billing_end,
                        'meter_category': properties.get('meterCategory'),
                        'meter_subcategory': properties.get('meterSubCategory'),
                        'meter_name': properties.get('meterName'),
                        'resource_name': resource_name,
                        'quantity': properties.get('quantity', 0),
                        'effective_price': properties.get('effectivePrice', 0),
                        'calculated_cost': calculated_cost
                    })
                
                print(f"  Records in file: {records_in_file}")
                print(f"  Total cost in file: ${file_total_cost:.2f}")
                
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON in file {file_path}: {e}")

    print(f"\nTotal records processed: {total_records}")
    print(f"Total calculated cost before DataFrame processing: ${sum(item['calculated_cost'] for item in all_data):.2f}")

    df = pd.DataFrame(all_data)

    # Ensure dates are properly parsed
    df['billing_period_start'] = pd.to_datetime(df['billing_period_start'], errors='coerce').dt.tz_localize(None)
    df['billing_period_end'] = pd.to_datetime(df['billing_period_end'], errors='coerce').dt.tz_localize(None)

    # Drop invalid dates (but they should be fixed now)
    df = df.dropna(subset=['billing_period_start', 'billing_period_end'])

    return df

def aggregate_and_compare_usage(df):
    """
    Aggregates the usage data by month and meter category, then prepares
    it for comparison.

    Args:
        df (pd.DataFrame): The usage data DataFrame.

    Returns:
        pd.DataFrame: Aggregated usage data for comparison.
    """
    # Debug: Check date parsing issues
    print(f"\nDataFrame shape before date filtering: {df.shape}")
    print(f"Date range in data: {df['billing_period_start'].min()} to {df['billing_period_start'].max()}")
    
    # Extract year-month for grouping, without timezone warning
    df['billing_month'] = df['billing_period_start'].dt.to_period('M')
    
    # Debug: Show month distribution before aggregation
    month_totals = df.groupby('billing_month')['calculated_cost'].sum()
    print(f"\nCost totals by month BEFORE aggregation:")
    for month, total in month_totals.items():
        print(f"  {month}: ${total:.2f}")
    
    # Check for May 2025 specifically
    may_2025_data = df[df['billing_month'] == '2025-05']
    print(f"\nMay 2025 debug:")
    print(f"  Records: {len(may_2025_data)}")
    print(f"  Total cost: ${may_2025_data['calculated_cost'].sum():.2f}")
    if len(may_2025_data) > 0:
        print(f"  Date range: {may_2025_data['billing_period_start'].min()} to {may_2025_data['billing_period_start'].max()}")

    # Separate storage accounts by resource name for detailed comparison
    df['meter_category_with_account'] = df.apply(
        lambda row: f"{row['meter_category']} ({row['resource_name']})" if row['meter_category'] == 'Storage' else row['meter_category'],
        axis=1
    )

    # Aggregate data by billing month and detailed meter category
    aggregated_data = df.groupby(['billing_month', 'meter_category_with_account']).agg({
        'calculated_cost': 'sum'
    }).unstack(fill_value=0)

    # Flatten the column hierarchy
    aggregated_data.columns = aggregated_data.columns.get_level_values(1)

    # Add a Storage Total column
    storage_columns = [col for col in aggregated_data.columns if col.startswith('Storage')]
    aggregated_data['Storage Total'] = aggregated_data[storage_columns].sum(axis=1)

    # Add a Total column (exclude Storage Total to avoid double counting)
    total_columns = [col for col in aggregated_data.columns if col != 'Storage Total']
    aggregated_data['Total'] = aggregated_data[total_columns].sum(axis=1)

    # Round all amounts to two decimal places
    aggregated_data = aggregated_data.round(2)

    return aggregated_data

def main():
    if len(sys.argv) != 4:
        print("Usage: python compare_usage_details.py <usage_directory> <usage_file_pattern> <compare_usage_details_report_file_path>")
        sys.exit(1)

    usage_directory = sys.argv[1]
    usage_file_pattern = sys.argv[2]
    report_file_path = sys.argv[3]

    # Load and process usage details
    usage_df = load_usage_details(usage_directory, usage_file_pattern)

    # Aggregate and compare usage data
    aggregated_data = aggregate_and_compare_usage(usage_df)

    # Save the results to a file
    aggregated_data.to_csv(report_file_path)

    # Display the results
    print("Aggregated Usage Data has been saved to:", report_file_path)

if __name__ == "__main__":
    main()
