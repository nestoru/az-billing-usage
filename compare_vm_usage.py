import json
import pandas as pd
import sys
import argparse

def extract_costs(json_data):
    records = []
    for entry in json_data.get('value', []):
        props = entry.get('properties', {})
        meter = props.get('meterCategory', '')
        if meter in ["Virtual Machines", "Virtual Machines Licenses"]:
            instance_raw = props.get('instanceName', '')
            instance = instance_raw.split("/")[-1] or instance_raw or "unknown"
            records.append({
                "instance": instance,
                "meter_category": meter,
                "cost": props.get('costInBillingCurrency', 0)
            })
    df = pd.DataFrame(records)
    return df.groupby(['instance', 'meter_category'])['cost'].sum().unstack(fill_value=0)

def main():
    parser = argparse.ArgumentParser(description='Compare VM costs between two billing periods')
    parser.add_argument('old_file', help='Path to the old period JSON file')
    parser.add_argument('new_file', help='Path to the new period JSON file')
    parser.add_argument('--output-csv', help='Path to save results as CSV file')
    
    args = parser.parse_args()
    
    with open(args.old_file) as f1:
        data1 = json.load(f1)
    with open(args.new_file) as f2:
        data2 = json.load(f2)

    df1 = extract_costs(data1)
    df2 = extract_costs(data2)

    # Join all VMs (not just those with increases)
    merged = df2.join(df1, lsuffix='_new', rsuffix='_old', how='outer').fillna(0)
    merged['total_old'] = merged.get('Virtual Machines_old', 0) + merged.get('Virtual Machines Licenses_old', 0)
    merged['total_new'] = merged.get('Virtual Machines_new', 0) + merged.get('Virtual Machines Licenses_new', 0)
    merged['difference'] = merged['total_new'] - merged['total_old']

    # Sort by difference (highest first) to see biggest cost increases at top
    result = merged.sort_values(by='difference', ascending=False)
    
    # Set pandas display options to show all rows and columns
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)
    
    print("="*120)
    print("COMPLETE VM COST COMPARISON - ALL INSTANCES")
    print("="*120)
    print(f"Period 1 (Old): {args.old_file}")
    print(f"Period 2 (New): {args.new_file}")
    print("="*120)
    
    # Display all results with clear formatting
    display_df = result[['Virtual Machines_old', 'Virtual Machines Licenses_old',
                        'Virtual Machines_new', 'Virtual Machines Licenses_new',
                        'total_old', 'total_new', 'difference']].round(2)
    print(display_df)
    
    # Save to CSV if requested
    if args.output_csv:
        display_df.to_csv(args.output_csv)
        print(f"\nResults saved to: {args.output_csv}")
    
    print("\n" + "="*120)
    print("SUMMARY STATISTICS")
    print("="*120)
    print(f"Total VMs found: {len(result)}")
    print(f"Total old period cost: ${result['total_old'].sum():.2f}")
    print(f"Total new period cost: ${result['total_new'].sum():.2f}")
    print(f"Overall difference: ${result['difference'].sum():.2f}")
    print(f"VMs with increased costs: {len(result[result['difference'] > 0])}")
    print(f"VMs with decreased costs: {len(result[result['difference'] < 0])}")
    print(f"VMs with no change: {len(result[result['difference'] == 0])}")
    print(f"New VMs (not in old period): {len(result[result['total_old'] == 0])}")
    print(f"Removed VMs (not in new period): {len(result[result['total_new'] == 0])}")

if __name__ == "__main__":
    main()
