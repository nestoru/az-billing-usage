#!/usr/bin/env python3

import json
import sys
import re
from datetime import datetime
from collections import defaultdict

def load_usage_data(file_path):
    """Load and parse the Azure usage JSON file."""
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        return data.get('value', [])
    except Exception as e:
        print(f"Error loading file {file_path}: {e}")
        sys.exit(1)

def extract_reservation_id(instance_name):
    """Extract reservation ID from instance name."""
    match = re.search(r'reservationOrders/([^/]+)', instance_name)
    return match.group(1) if match else None

def extract_vm_size(product_name):
    """Extract VM size from product name."""
    match = re.search(r'Standard_[A-Za-z0-9_]+', product_name)
    return match.group(0) if match else 'Unknown'

def analyze_reservations(usage_data):
    """Analyze all reservations and their usage."""
    
    # Find all reservation records
    reservation_records = {}
    consumption_records = defaultdict(list)
    vm_records = defaultdict(list)
    
    for record in usage_data:
        props = record.get('properties', {})
        instance_name = props.get('instanceName', '')
        
        if 'reservationOrders' in instance_name:
            reservation_id = extract_reservation_id(instance_name)
            if reservation_id and props.get('chargeType') == 'Purchase':
                reservation_records[reservation_id] = record
        
        # Look for consumption records (VM and Database)
        additional_info = props.get('additionalInfo', '')
        if additional_info:
            try:
                info_data = json.loads(additional_info)
                if 'ReservationOrderId' in info_data:
                    res_id = info_data['ReservationOrderId']
                    # For VMs, look for ConsumedQuantity
                    if 'ConsumedQuantity' in info_data:
                        consumption_records[res_id].append(info_data)
                    # For Databases, ReservationOrderId presence indicates usage
                    elif 'ReservationId' in info_data:
                        consumption_records[res_id].append({
                            'service_type': 'Database',
                            'reservation_id': info_data['ReservationId'],
                            'consumed': True
                        })
            except:
                pass
        
        # Collect VM records for matching
        if 'reservationOrders' not in instance_name and props.get('meterCategory') == 'Virtual Machines':
            vm_records[instance_name].append(record)
    
    return reservation_records, consumption_records, vm_records

def find_beneficiary_resources(reservation_product, reservation_region, vm_records, usage_data):
    """Find resources (VMs or databases) that benefit from a specific reservation."""
    vm_size = extract_vm_size(reservation_product)
    beneficiaries = []
    
    # Handle VM reservations
    if "Virtual Machines" in reservation_product or "VM Instance" in reservation_product:
        for vm_name, records in vm_records.items():
            for record in records:
                props = record.get('properties', {})
                if (props.get('meterRegion') == reservation_region and 
                    props.get('additionalInfo')):
                    try:
                        info = json.loads(props.get('additionalInfo', '{}'))
                        if info.get('ServiceType') == vm_size:
                            cost = props.get('costInBillingCurrency', 0)
                            beneficiaries.append({
                                'resource_name': vm_name,
                                'cost': cost,
                                'resource_type': vm_size
                            })
                    except:
                        pass
    
    # Handle Database reservations (PostgreSQL, SQL, etc.)
    elif "Database" in reservation_product or "PostgreSQL" in reservation_product:
        # Look for database resources that match the reservation
        for record in usage_data:
            props = record.get('properties', {})
            instance_name = props.get('instanceName', '')
            
            # Skip reservation billing records
            if 'reservationOrders' in instance_name:
                continue
                
            # Look for database resources
            consumed_service = props.get('consumedService', '')
            meter_region = props.get('meterRegion', '')
            meter_subcategory = props.get('meterSubCategory', '')
            
            if (consumed_service == 'Microsoft.DBforPostgreSQL' and 
                meter_region == reservation_region and
                'General Purpose Dadsv5 Series Compute' in meter_subcategory):
                
                cost = props.get('costInBillingCurrency', 0)
                effective_price = props.get('effectivePrice', 0)
                payg_price = props.get('payGPrice', 0)
                
                # Check if reservation discount is applied
                is_discounted = effective_price < payg_price if payg_price > 0 else False
                
                beneficiaries.append({
                    'resource_name': instance_name,
                    'cost': cost,
                    'resource_type': 'PostgreSQL Database',
                    'effective_price': effective_price,
                    'payg_price': payg_price,
                    'is_discounted': is_discounted
                })
    
    # Aggregate costs by resource
    resource_costs = defaultdict(lambda: {'cost': 0, 'details': {}})
    for b in beneficiaries:
        resource_name = b['resource_name']
        resource_costs[resource_name]['cost'] += b['cost']
        if 'is_discounted' in b:
            resource_costs[resource_name]['details'] = {
                'effective_price': b['effective_price'],
                'payg_price': b['payg_price'],
                'is_discounted': b['is_discounted']
            }
    
    return [(resource, data['cost'], data['details']) for resource, data in resource_costs.items()]

def generate_report(usage_file):
    """Generate the complete reservations report."""
    
    print("Azure Reservations Analysis Report")
    print("=" * 50)
    print(f"Generated: {datetime.now()}")
    print(f"Source: {usage_file}")
    print()
    
    usage_data = load_usage_data(usage_file)
    reservation_records, consumption_records, vm_records = analyze_reservations(usage_data)
    
    total_cost = 0
    used_count = 0
    unused_count = 0
    used_cost = 0
    unused_cost = 0
    
    # Analyze each reservation
    for reservation_id, record in reservation_records.items():
        props = record.get('properties', {})
        
        cost = props.get('costInBillingCurrency', 0)
        product = props.get('product', 'Unknown')
        region = props.get('meterRegion', 'Unknown')
        subcategory = props.get('meterSubCategory', 'Unknown')
        start_date = props.get('servicePeriodStartDate', 'Unknown')
        end_date = props.get('servicePeriodEndDate', 'Unknown')
        
        print(f"RESERVATION: {reservation_id}")
        print("=" * 50)
        print(f"Cost: ${cost:.2f}/month")
        print(f"Product: {product}")
        print(f"Region: {region}")
        print(f"Category: {subcategory}")
        print(f"Period: {start_date} to {end_date}")
        print()
        
        # Check for consumption
        consumption = consumption_records.get(reservation_id, [])
        
        if consumption:
            print("✅ STATUS: USED - Found consumption records")
            
            # Calculate total consumed for VMs
            vm_consumed = sum(float(c.get('ConsumedQuantity', 0)) for c in consumption if 'ConsumedQuantity' in c)
            db_consumed = len([c for c in consumption if c.get('service_type') == 'Database'])
            
            if vm_consumed > 0:
                print(f"📊 Total VM Consumed Quantity: {vm_consumed:.2f} hours")
            if db_consumed > 0:
                print(f"📊 Database Usage Records: {db_consumed} consumption entries")
            
            # Find beneficiary resources
            beneficiaries = find_beneficiary_resources(product, region, vm_records, usage_data)
            
            if beneficiaries:
                print("🔍 BENEFICIARY RESOURCES:")
                for resource_name, resource_cost, details in beneficiaries:
                    if details and 'is_discounted' in details:
                        # Database resource with pricing details
                        discount_status = "✅ RESERVED PRICING" if not details['is_discounted'] else "✅ DISCOUNTED"
                        print(f"   • {resource_name}: ${resource_cost:.2f} ({discount_status})")
                        if details['effective_price'] == details['payg_price']:
                            print(f"     Using reserved pricing: ${details['effective_price']:.3f}/hour")
                    else:
                        # VM resource
                        print(f"   • {resource_name}: ${resource_cost:.2f} (discounted by reservation)")
            else:
                print("🔍 Reservation is being consumed (consumption records found)")
            
            used_count += 1
            used_cost += cost
        else:
            print("❌ STATUS: UNUSED - No consumption records found")
            print(f"💰 WASTE: ${cost:.2f}/month (${cost * 12:.2f}/year)")
            unused_count += 1
            unused_cost += cost
        
        total_cost += cost
        print()
    
    # Summary
    print("SUMMARY")
    print("=" * 20)
    print(f"Total Reservations: {len(reservation_records)}")
    print(f"Used Reservations: {used_count} (${used_cost:.2f}/month)")
    print(f"Unused Reservations: {unused_count} (${unused_cost:.2f}/month)")
    print(f"Total Cost: ${total_cost:.2f}/month (${total_cost * 12:.2f}/year)")
    print(f"Annual Waste: ${unused_cost * 12:.2f}")
    print()
    
    if unused_count > 0:
        print("🚨 RECOMMENDATIONS:")
        print("=" * 20)
        print(f"1. Cancel unused reservations to save ${unused_cost * 12:.2f}/year")
        print("2. Consider purchasing reservations for VMs currently on pay-as-you-go")
        print("3. Review reservation sizes and regions for optimal coverage")
    
    print("\n✅ Analysis complete!")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 reservations_report.py /path/to/az_usage_details.json")
        sys.exit(1)
    
    usage_file = sys.argv[1]
    generate_report(usage_file)
