#!/usr/bin/env python3

import json
import pandas as pd
import sys
import argparse
import os

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
    
    if not records:
        # Return empty DataFrame with expected columns if no VM records found
        return pd.DataFrame(columns=['Virtual Machines', 'Virtual Machines Licenses'])
    
    df = pd.DataFrame(records)
    return df.groupby(['instance', 'meter_category'])['cost'].sum().unstack(fill_value=0)

def main():
    parser = argparse.ArgumentParser(description='Compare VM costs between two billing periods')
    parser.add_argument('old_file', help='Path to the old period JSON file')
    parser.add_argument('new_file', help='Path to the new period JSON file')
    parser.add_argument('--output-csv', help='Path to save results as CSV file')
    
    args = parser.parse_args()
    
    # Validate input files exist
    if not os.path.exists(args.old_file):
        print(f"Error: File '{args.old_file}' not found.")
        sys.exit(1)
    
    if not os.path.exists(args.new_file):
        print(f"Error: File '{args.new_file}' not found.")
        sys.exit(1)
    
    # Load JSON files with error handling
    try:
        with open(args.old_file) as f1:
            data1 = json.load(f1)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in '{args.old_file}': {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading '{args.old_file}': {e}")
        sys.exit(1)
    
    try:
        with open(args.new_file) as f2:
            data2 = json.load(f2)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in '{args.new_file}': {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading '{args.new_file}': {e}")
        sys.exit(1)
    
    df1 = extract_costs(data1)
    df2 = extract_costs(data2)
    
    # Handle case where one or both DataFrames are empty
    if df1.empty and df2.empty:
        print("No VM data found in either file.")
        sys.exit(1)
    
    # Join all VMs (not just those with increases)
    merged = df2.join(df1, lsuffix='_new', rsuffix='_old', how='outer').fillna(0)
    
    # Handle missing columns gracefully
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
    display_columns = []
    if 'Virtual Machines_old' in result.columns:
        display_columns.append('Virtual Machines_old')
    if 'Virtual Machines Licenses_old' in result.columns:
        display_columns.append('Virtual Machines Licenses_old')
    if 'Virtual Machines_new' in result.columns:
        display_columns.append('Virtual Machines_new')
    if 'Virtual Machines Licenses_new' in result.columns:
        display_columns.append('Virtual Machines Licenses_new')
    
    display_columns.extend(['total_old', 'total_new', 'difference'])
    
    display_df = result[display_columns].round(2)
    print(display_df)
    
    # Save to CSV if requested
    if args.output_csv:
        try:
            display_df.to_csv(args.output_csv)
            print(f"\nResults saved to: {args.output_csv}")
        except Exception as e:
            print(f"\nError saving CSV: {e}")
    
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
