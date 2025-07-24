#!/usr/bin/env python3
"""
Azure Storage Billing Analysis with Capacity vs Usage Tracking
Properly distinguishes between provisioned capacity and actual usage
"""

import json
import sys
import os
from collections import defaultdict
from datetime import datetime
import csv

def load_billing_data(file_path):
    """Load Azure billing data from JSON file"""
    # Validate file exists
    if not os.path.exists(file_path):
        print(f"Error: File not found: {file_path}")
        return []
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        if isinstance(data, dict) and 'value' in data:
            return data['value']
        elif isinstance(data, list):
            return data
        else:
            print(f"Unexpected data format in {file_path}")
            return []
            
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON in {file_path}: {e}")
        return []
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return []

def is_storage_related(record):
    """Determine if a billing record is storage-related"""
    properties = record.get('properties', {})
    meter_category = properties.get('meterCategory', '')
    meter_subcategory = properties.get('meterSubCategory', '')
    meter_name = properties.get('meterName', '')
    
    # Storage category
    if meter_category == 'Storage':
        return True
    
    # Backup category
    if meter_category == 'Backup':
        return True
    
    # Disk-related charges in other categories
    disk_keywords = ['disk', 'ssd', 'hdd', 'managed disk', 'snapshot']
    if any(keyword.lower() in meter_subcategory.lower() for keyword in disk_keywords):
        return True
    if any(keyword.lower() in meter_name.lower() for keyword in disk_keywords):
        return True
    
    return False

def extract_instance_name(full_path):
    """Extract resource name from Azure resource path"""
    if isinstance(full_path, str) and '/' in full_path:
        return full_path.split('/')[-1]
    return str(full_path) if full_path else 'unknown'

def analyze_meter_data(properties):
    """
    Analyze meter data to extract capacity, usage, and storage type
    Returns (storage_type, provisioned_capacity_gb, actual_usage_gb, billing_model)
    """
    meter_category = properties.get('meterCategory', '')
    meter_subcategory = properties.get('meterSubCategory', '')
    meter_name = properties.get('meterName', '')
    quantity = float(properties.get('quantity', 0))
    unit_of_measure = properties.get('unitOfMeasure', '')
    
    # Backup services - quantity represents actual usage
    if 'Backup' in meter_category:
        return 'Backup', 0, quantity, 'usage_based'
    
    # Managed Disks - provisioned capacity model
    if 'Managed Disks' in meter_subcategory:
        # Standard disk tier capacities (what you pay for regardless of usage)
        disk_capacities = {
            'P4': 32, 'P6': 64, 'P10': 128, 'P15': 256, 'P20': 512,
            'P30': 1024, 'P40': 2048, 'P50': 4096, 'P60': 8192,
            'P70': 16384, 'P80': 32768,
            'S4': 32, 'S6': 64, 'S10': 128, 'S15': 256, 'S20': 512,
            'S30': 1024, 'S40': 2048, 'S50': 4096, 'S60': 8192,
            'S70': 16384, 'S80': 32768,
            'E4': 32, 'E6': 64, 'E10': 128, 'E15': 256, 'E20': 512,
            'E30': 1024, 'E40': 2048, 'E50': 4096, 'E60': 8192,
            'E70': 16384, 'E80': 32768
        }
        
        # Look for standard tier in meter name
        for tier, capacity_gb in disk_capacities.items():
            if f'{tier} LRS' in meter_name or f'{tier} ZRS' in meter_name or f'{tier} GRS' in meter_name:
                # For managed disks, provisioned capacity = what you pay for
                # Actual usage = unknown (Azure doesn't bill based on usage for managed disks)
                return 'VM_Disk', capacity_gb, 0, 'provisioned'
        
        # Custom disk sizes - try to extract from meter name or use quantity
        # This is trickier and may need adjustment based on your specific data
        if 'hour' in unit_of_measure.lower():
            # Quantity is billing hours, try to extract disk size from name or other field
            # For now, we'll estimate based on quantity patterns
            estimated_capacity = quantity / 24 / 30 if quantity > 1000 else quantity
            return 'VM_Disk', estimated_capacity, 0, 'provisioned'
        else:
            return 'VM_Disk', quantity, 0, 'provisioned'
    
    # Snapshots - actual usage based
    if 'Snapshot' in meter_name or 'Snapshot' in meter_subcategory:
        if any(disk_type in meter_subcategory for disk_type in ['Premium SSD', 'Standard SSD', 'Standard HDD']):
            return 'VM_Disk', 0, quantity, 'usage_based'
        else:
            return 'File_Storage', 0, quantity, 'usage_based'
    
    # File Storage, Blob Storage, etc. - usage based
    if any(storage_type in meter_subcategory for storage_type in 
           ['Blob', 'Files', 'File Sync', 'Tables', 'Queues', 'Cool', 'Hot', 'Archive']):
        return 'File_Storage', 0, quantity, 'usage_based'
    
    # Default to Storage with actual usage
    return 'Storage', 0, quantity, 'usage_based'

def process_billing_data(records):
    """Process billing records tracking both capacity and usage"""
    storage_data = defaultdict(lambda: {
        'cost': 0,
        'provisioned_capacity_gb': 0,  # What you're paying for (disk size)
        'actual_usage_gb': 0,          # What you're actually using
        'storage_types': set(),
        'billing_models': set(),
        'meter_details': []
    })
    
    total_cost = 0
    total_provisioned_gb = 0
    total_usage_gb = 0
    
    for record in records:
        if not is_storage_related(record):
            continue
            
        properties = record.get('properties', {})
        instance_name = extract_instance_name(properties.get('instanceName', ''))
        
        try:
            cost = float(properties.get('costInBillingCurrency', 0))
        except (ValueError, TypeError):
            cost = 0
        
        storage_type, provisioned_gb, usage_gb, billing_model = analyze_meter_data(properties)
        
        # Aggregate data per instance
        storage_data[instance_name]['cost'] += cost
        storage_data[instance_name]['storage_types'].add(storage_type)
        storage_data[instance_name]['billing_models'].add(billing_model)
        
        # Handle capacity vs usage differently based on billing model
        if billing_model == 'provisioned':
            # For provisioned resources, take the max capacity (you pay for the full resource)
            storage_data[instance_name]['provisioned_capacity_gb'] = max(
                storage_data[instance_name]['provisioned_capacity_gb'], provisioned_gb
            )
        else:
            # For usage-based resources, sum the actual usage
            storage_data[instance_name]['actual_usage_gb'] += usage_gb
        
        storage_data[instance_name]['meter_details'].append({
            'meter_name': properties.get('meterName', ''),
            'meter_subcategory': properties.get('meterSubCategory', ''),
            'provisioned_gb': provisioned_gb,
            'usage_gb': usage_gb,
            'cost': cost,
            'billing_model': billing_model
        })
        
        total_cost += cost
        total_provisioned_gb += provisioned_gb
        total_usage_gb += usage_gb
    
    return storage_data, total_cost, total_provisioned_gb, total_usage_gb

def generate_comparison_report(old_data, new_data, old_totals, new_totals):
    """Generate comparison report showing both capacity and usage"""
    all_instances = set(old_data.keys()) | set(new_data.keys())
    
    report_lines = []
    summary_stats = {
        'increased': 0, 'decreased': 0, 'unchanged': 0,
        'total_old_cost': old_totals[0], 'total_new_cost': new_totals[0],
        'total_old_provisioned': old_totals[1], 'total_new_provisioned': new_totals[1],
        'total_old_usage': old_totals[2], 'total_new_usage': new_totals[2]
    }
    
    for instance in all_instances:
        old_item = old_data.get(instance, {
            'cost': 0, 'provisioned_capacity_gb': 0, 'actual_usage_gb': 0, 
            'storage_types': set(), 'billing_models': set()
        })
        new_item = new_data.get(instance, {
            'cost': 0, 'provisioned_capacity_gb': 0, 'actual_usage_gb': 0, 
            'storage_types': set(), 'billing_models': set()
        })
        
        # Extract values
        old_cost = old_item['cost']
        old_provisioned = old_item['provisioned_capacity_gb']
        old_usage = old_item['actual_usage_gb']
        
        new_cost = new_item['cost']
        new_provisioned = new_item['provisioned_capacity_gb']
        new_usage = new_item['actual_usage_gb']
        
        # Calculate differences
        cost_diff = new_cost - old_cost
        provisioned_diff = new_provisioned - old_provisioned
        usage_diff = new_usage - old_usage
        
        # Get storage types
        storage_types = old_item['storage_types'] | new_item['storage_types']
        storage_type_str = ', '.join(sorted(storage_types)) if storage_types else 'Unknown'
        
        # Determine primary metric to display (provisioned capacity or actual usage)
        billing_models = old_item['billing_models'] | new_item['billing_models']
        is_provisioned = 'provisioned' in billing_models
        
        # Debug: For backup items, ensure they're marked as usage-based
        if 'Backup' in storage_type_str:
            is_provisioned = False
        
        # Generate optimization recommendation
        optimization_note = get_optimization_recommendation(
            new_cost, new_provisioned, new_usage, storage_type_str, is_provisioned
        )
        
        # Track summary statistics
        if cost_diff > 0.01:
            summary_stats['increased'] += 1
        elif cost_diff < -0.01:
            summary_stats['decreased'] += 1
        else:
            summary_stats['unchanged'] += 1
        
        report_lines.append({
            'instance': instance,
            'new_cost': new_cost,
            'new_provisioned_gb': new_provisioned,
            'new_usage_gb': new_usage,
            'old_cost': old_cost,
            'old_provisioned_gb': old_provisioned,
            'old_usage_gb': old_usage,
            'cost_diff': cost_diff,
            'provisioned_diff': provisioned_diff,
            'usage_diff': usage_diff,
            'storage_type': storage_type_str,
            'is_provisioned': is_provisioned,
            'optimization_note': optimization_note
        })
    
    # Sort by cost difference (highest increases first)
    report_lines.sort(key=lambda x: x['cost_diff'], reverse=True)
    
    return report_lines, summary_stats

def get_optimization_recommendation(cost, provisioned_gb, usage_gb, storage_type, is_provisioned):
    """Generate optimization recommendations based on capacity vs usage"""
    if cost < 10:
        return "Low impact"
    
    if is_provisioned and provisioned_gb > 0:
        # For provisioned resources, we can't see usage but can check if over-provisioned
        if cost > 1000:
            return "HIGH: Review if full capacity needed"
        elif cost > 200:
            return "MEDIUM: Check actual disk utilization"
        else:
            return "Monitor capacity needs"
    
    elif usage_gb > 0:
        cost_per_gb = cost / usage_gb
        
        if 'File_Storage' in storage_type or 'Storage' in storage_type:
            if cost_per_gb > 0.15:
                return "HIGH: Consider archive/cool tiers"
            elif cost > 500:
                return "MEDIUM: Review lifecycle policies"
            else:
                return "Monitor growth trends"
        
        elif 'Backup' in storage_type:
            if cost > 2000:
                return "HIGH: Review retention policies"
            elif cost > 500:
                return "MEDIUM: Optimize backup frequency"
            else:
                return "Review retention settings"
    
    return "Review usage patterns"

def save_csv_report(report_lines, filename):
    """Save detailed comparison report to CSV"""
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = [
                'instance', 'new_cost', 'new_provisioned_gb', 'new_usage_gb',
                'old_cost', 'old_provisioned_gb', 'old_usage_gb',
                'cost_diff', 'provisioned_diff', 'usage_diff',
                'storage_type', 'billing_model', 'optimization_note'
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            
            for line in report_lines:
                writer.writerow({
                    'instance': line['instance'],
                    'new_cost': f"{line['new_cost']:.2f}",
                    'new_provisioned_gb': f"{line['new_provisioned_gb']:.0f}",
                    'new_usage_gb': f"{line['new_usage_gb']:.2f}",
                    'old_cost': f"{line['old_cost']:.2f}",
                    'old_provisioned_gb': f"{line['old_provisioned_gb']:.0f}",
                    'old_usage_gb': f"{line['old_usage_gb']:.2f}",
                    'cost_diff': f"{line['cost_diff']:.2f}",
                    'provisioned_diff': f"{line['provisioned_diff']:.0f}",
                    'usage_diff': f"{line['usage_diff']:.2f}",
                    'storage_type': line['storage_type'],
                    'billing_model': 'Provisioned' if line['is_provisioned'] else 'Usage-based',
                    'optimization_note': line['optimization_note']
                })
    except Exception as e:
        print(f"Error saving CSV file: {e}")

def print_comparison_report(report_lines, summary_stats):
    """Print formatted comparison report with capacity vs usage"""
    
    # Header
    print(f"{'Instance':<50} {'New Cost ($)':>12} {'New Prov (GB)':>13} {'New Usage (GB)':>14} {'Old Cost ($)':>12} {'Old Prov (GB)':>13} {'Old Usage (GB)':>14} {'Cost Δ ($)':>11} {'Type':>12} {'Optimization':<20}")
    print("-" * 175)
    
    # Data rows
    for line in report_lines:
        # Show the relevant metric based on billing model
        if line['is_provisioned']:
            # For provisioned resources, show capacity, usage as N/A
            new_prov = f"{line['new_provisioned_gb']:.0f}"
            old_prov = f"{line['old_provisioned_gb']:.0f}"
            new_usage = "N/A"
            old_usage = "N/A"
        else:
            # For usage-based resources, show N/A for provisioned, actual usage
            new_prov = "N/A"
            old_prov = "N/A"
            new_usage = f"{line['new_usage_gb']:.1f}"
            old_usage = f"{line['old_usage_gb']:.1f}"
        
        print(f"{line['instance'][:49]:<50} "
              f"{line['new_cost']:>11.2f} "
              f"{new_prov:>13} "
              f"{new_usage:>14} "
              f"{line['old_cost']:>11.2f} "
              f"{old_prov:>13} "
              f"{old_usage:>14} "
              f"{line['cost_diff']:>10.2f} "
              f"{line['storage_type'][:11]:>12} "
              f"{line['optimization_note'][:19]:<20}")
    
    # Summary
    print("\n" + "="*120)
    print("SUMMARY STATISTICS")
    print("="*120)
    print(f"Total storage resources: {len(report_lines)}")
    print("\nCOST COMPARISON:")
    print(f"  New period: ${summary_stats['total_new_cost']:,.2f}")
    print(f"  Old period: ${summary_stats['total_old_cost']:,.2f}")
    print(f"  Difference: ${summary_stats['total_new_cost'] - summary_stats['total_old_cost']:,.2f}")
    
    print("\nCAPACITY vs USAGE:")
    print(f"  Provisioned capacity (new): {summary_stats['total_new_provisioned']:,.0f} GB")
    print(f"  Provisioned capacity (old): {summary_stats['total_old_provisioned']:,.0f} GB")
    print(f"  Actual usage (new): {summary_stats['total_new_usage']:,.1f} GB")
    print(f"  Actual usage (old): {summary_stats['total_old_usage']:,.1f} GB")
    
    print(f"\nCost changes: {summary_stats['increased']} increased, {summary_stats['decreased']} decreased, {summary_stats['unchanged']} unchanged")
    
    # Usage efficiency analysis
    print("\n" + "="*120)
    print("USAGE EFFICIENCY ANALYSIS")
    print("="*120)
    
    provisioned_items = [item for item in report_lines if item['is_provisioned'] and item['new_provisioned_gb'] > 0]
    usage_items = [item for item in report_lines if not item['is_provisioned'] and item['new_usage_gb'] > 0]
    
    print(f"Provisioned resources: {len(provisioned_items)} items (you pay regardless of usage)")
    print(f"Usage-based resources: {len(usage_items)} items (you pay for what you use)")
    
    if provisioned_items:
        total_provisioned_cost = sum(item['new_cost'] for item in provisioned_items)
        print(f"Total provisioned cost: ${total_provisioned_cost:,.2f} (potential for right-sizing)")
    
    if usage_items:
        total_usage_cost = sum(item['new_cost'] for item in usage_items)
        print(f"Total usage-based cost: ${total_usage_cost:,.2f} (potential for lifecycle optimization)")

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Compare Azure storage costs with capacity vs usage analysis')
    parser.add_argument('old_period_file', help='JSON file for old period')
    parser.add_argument('new_period_file', help='JSON file for new period')
    parser.add_argument('--output-csv', help='Output CSV file path')
    
    args = parser.parse_args()
    
    print(f"Loading old period data from: {args.old_period_file}")
    old_records = load_billing_data(args.old_period_file)
    if not old_records:
        print("Error: Could not load old period data")
        sys.exit(1)
    print(f"Loaded {len(old_records)} records from old period")
    
    print(f"Loading new period data from: {args.new_period_file}")
    new_records = load_billing_data(args.new_period_file)
    if not new_records:
        print("Error: Could not load new period data")
        sys.exit(1)
    print(f"Loaded {len(new_records)} records from new period")
    
    print("\nProcessing data...")
    old_data, old_cost, old_provisioned, old_usage = process_billing_data(old_records)
    new_data, new_cost, new_provisioned, new_usage = process_billing_data(new_records)
    
    print(f"Old period: {len(old_data)} storage resources, ${old_cost:.2f} total cost")
    print(f"New period: {len(new_data)} storage resources, ${new_cost:.2f} total cost")
    
    print("\nGenerating comparison report...")
    report_lines, summary_stats = generate_comparison_report(
        old_data, new_data,
        (old_cost, old_provisioned, old_usage),
        (new_cost, new_provisioned, new_usage)
    )
    
    # Print report
    print_comparison_report(report_lines, summary_stats)
    
    # Save to CSV
    if args.output_csv:
        csv_filename = args.output_csv
    else:
        csv_filename = f"storage_capacity_usage_comparison_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    save_csv_report(report_lines, csv_filename)
    print(f"\nDetailed results saved to: {csv_filename}")

if __name__ == "__main__":
    main()
