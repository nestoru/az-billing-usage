#!/usr/bin/env python3
"""
Enhanced Azure Usage Details Fetcher
Fetches consumption data with pay-as-you-go pricing for accurate savings calculations
"""

import json
import subprocess
import argparse
import sys
from datetime import datetime

def fetch_usage_details(subscription_id, start_date, end_date, api_version="2023-05-01", include_meter_details=True):
    """
    Fetch Azure usage details with enhanced pricing information.
    
    Args:
        subscription_id: Azure subscription ID
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        api_version: API version (2023-05-01 includes payGPrice)
        include_meter_details: Whether to fetch additional meter details
    """
    
    # Build the URL with the newer API version
    base_url = f"https://management.azure.com/subscriptions/{subscription_id}/providers/Microsoft.Consumption/usageDetails"
    
    # Use the newer API parameters
    params = [
        f"startDate={start_date}",
        f"endDate={end_date}",
        f"api-version={api_version}",
        "$expand=meterDetails,additionalInfo,marketPlace"  # Request expanded details
    ]
    
    url = f"{base_url}?{'&'.join(params)}"
    
    print(f"Fetching usage details from {start_date} to {end_date}...", file=sys.stderr)
    print(f"Using API version: {api_version}", file=sys.stderr)
    
    results = []
    page_count = 0
    
    while url:
        page_count += 1
        print(f"Fetching page {page_count}...", file=sys.stderr)
        
        command = ['az', 'rest', '--method', 'GET', '--url', url, '--output', 'json']
        
        # Execute the az rest command
        result = subprocess.run(command, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"Error: {result.stderr}", file=sys.stderr)
            # Try fallback to older API if new one fails
            if api_version == "2023-05-01":
                print("Falling back to API version 2021-10-01...", file=sys.stderr)
                return fetch_usage_details(subscription_id, start_date, end_date, "2021-10-01", include_meter_details)
            sys.exit(1)
        
        # Parse the JSON response
        try:
            data = json.loads(result.stdout)
        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}", file=sys.stderr)
            sys.exit(1)
        
        if 'error' in data:
            error_msg = data['error'].get('message', 'Unknown error')
            print(f"API error: {error_msg}", file=sys.stderr)
            
            # If the error is about API version, try an older one
            if 'api-version' in error_msg.lower() and api_version == "2023-05-01":
                print("Falling back to API version 2021-10-01...", file=sys.stderr)
                return fetch_usage_details(subscription_id, start_date, end_date, "2021-10-01", include_meter_details)
            sys.exit(1)
        
        # Add the current page of results to the list
        current_results = data.get('value', [])
        results.extend(current_results)
        
        print(f"  Retrieved {len(current_results)} records (Total: {len(results)})", file=sys.stderr)
        
        # Get the nextLink for pagination
        url = data.get('nextLink')
    
    print(f"\nTotal records fetched: {len(results)}", file=sys.stderr)
    
    # Post-process to ensure we have pricing data
    enhance_pricing_data(results)
    
    return {"value": results}

def enhance_pricing_data(results):
    """
    Post-process results to ensure pricing fields are present and properly formatted.
    """
    pricing_fields_found = 0
    
    for record in results:
        props = record.get('properties', {})
        
        # Check if we have PAYG pricing
        if 'payGPrice' in props:
            pricing_fields_found += 1
        
        # Ensure numeric fields are properly typed
        for field in ['costInBillingCurrency', 'payGPrice', 'effectivePrice', 'unitPrice', 'quantity']:
            if field in props and props[field] is not None:
                try:
                    props[field] = float(props[field])
                except (ValueError, TypeError):
                    pass
    
    if pricing_fields_found > 0:
        print(f"Found PAYG pricing in {pricing_fields_found}/{len(results)} records", file=sys.stderr)
    else:
        print("⚠️  Warning: No PAYG pricing data found. Savings calculations will not be possible.", file=sys.stderr)
        print("   This might be due to API version or subscription type limitations.", file=sys.stderr)

def fetch_price_sheet(subscription_id, billing_period=None):
    """
    Optionally fetch the price sheet for additional pricing information.
    """
    print("\nAttempting to fetch price sheet for additional pricing data...", file=sys.stderr)
    
    if billing_period:
        url = f"https://management.azure.com/subscriptions/{subscription_id}/providers/Microsoft.Billing/billingPeriods/{billing_period}/providers/Microsoft.Consumption/pricesheets/default?api-version=2023-05-01"
    else:
        url = f"https://management.azure.com/subscriptions/{subscription_id}/providers/Microsoft.Consumption/pricesheets/default?api-version=2023-05-01"
    
    command = ['az', 'rest', '--method', 'GET', '--url', url, '--output', 'json']
    result = subprocess.run(command, capture_output=True, text=True)
    
    if result.returncode == 0:
        try:
            price_data = json.loads(result.stdout)
            print(f"Successfully fetched price sheet with {len(price_data.get('properties', {}).get('pricesheets', []))} items", file=sys.stderr)
            return price_data
        except:
            print("Could not parse price sheet data", file=sys.stderr)
    else:
        print("Could not fetch price sheet (this is optional, continuing...)", file=sys.stderr)
    
    return None

def main():
    # Set up argument parsing
    parser = argparse.ArgumentParser(
        description='Fetch Azure consumption usage details with enhanced pricing information.',
        epilog='Example: %(prog)s 0a7c4fbf-b37a-41c1-8273-0f0496a5582f 2025-11-01 2025-11-30'
    )
    parser.add_argument('subscription_id', help='Azure Subscription ID')
    parser.add_argument('start_date', help='Start date in YYYY-MM-DD format')
    parser.add_argument('end_date', help='End date in YYYY-MM-DD format')
    parser.add_argument('--api-version', default='2023-05-01', 
                       help='API version to use (default: 2023-05-01 for PAYG pricing)')
    parser.add_argument('--include-price-sheet', action='store_true',
                       help='Also fetch the price sheet for additional pricing data')
    
    args = parser.parse_args()
    
    # Validate date format
    try:
        datetime.strptime(args.start_date, '%Y-%m-%d')
        datetime.strptime(args.end_date, '%Y-%m-%d')
    except ValueError:
        print("Error: Dates must be in YYYY-MM-DD format", file=sys.stderr)
        sys.exit(1)
    
    # Fetch usage details
    results = fetch_usage_details(
        args.subscription_id, 
        args.start_date, 
        args.end_date,
        args.api_version
    )
    
    # Optionally fetch price sheet
    if args.include_price_sheet:
        price_sheet = fetch_price_sheet(args.subscription_id)
        if price_sheet:
            results['priceSheet'] = price_sheet
    
    # Print the combined results as JSON to stdout
    print(json.dumps(results, indent=2))
    
    print(f"\n✅ Export complete. Pipe output to a file for analysis.", file=sys.stderr)

if __name__ == "__main__":
    main()

