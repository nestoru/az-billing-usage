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

def debug_reservations(usage_file):
    """Debug all reservation-related records."""
    
    print("AZURE RESERVATIONS DEBUG ANALYSIS")
    print("=" * 60)
    print(f"Generated: {datetime.now()}")
    print(f"Source: {usage_file}")
    print()
    
    usage_data = load_usage_data(usage_file)
    
    # 1. Find ALL reservation purchase records
    print("1. ALL RESERVATION PURCHASE RECORDS")
    print("=" * 50)
    
    reservation_purchases = []
    for i, record in enumerate(usage_data):
        props = record.get('properties', {})
        instance_name = props.get('instanceName', '')
        
        if 'reservationOrders' in instance_name and props.get('chargeType') == 'Purchase':
            reservation_id = extract_reservation_id(instance_name)
            reservation_purchases.append({
                'index': i,
                'reservation_id': reservation_id,
                'cost': props.get('costInBillingCurrency', 0),
                'product': props.get('product', ''),
                'region': props.get('meterRegion', ''),
                'category': props.get('meterSubCategory', ''),
                'instance_name': instance_name,
                'full_record': record
            })
    
    for res in reservation_purchases:
        print(f"Reservation ID: {res['reservation_id']}")
        print(f"  Cost: ${res['cost']:.2f}/month")
        print(f"  Product: {res['product']}")
        print(f"  Region: {res['region']}")
        print(f"  Category: {res['category']}")
        print(f"  Instance Name: {res['instance_name']}")
        print(f"  Record Index: {res['index']}")
        print()
    
    print(f"Total Reservations Found: {len(reservation_purchases)}")
    print()
    
    # 2. Focus on D8as_v5 reservations specifically
    print("2. D8as_v5 RESERVATIONS DETAILED ANALYSIS")
    print("=" * 50)
    
    d8as_reservations = [r for r in reservation_purchases if 'Standard_D8as_v5' in r['product']]
    
    if not d8as_reservations:
        print("❌ No D8as_v5 reservations found!")
        print("   Available VM sizes in reservations:")
        vm_products = [r['product'] for r in reservation_purchases if 'VM Instance' in r['product']]
        for product in set(vm_products):
            print(f"   - {product}")
    else:
        print(f"Found {len(d8as_reservations)} D8as_v5 reservation(s):")
        for res in d8as_reservations:
            print(f"  • {res['reservation_id']} - ${res['cost']:.2f}/month")
    print()
    
    # 3. Find ALL consumption records with ReservationOrderId
    print("3. ALL CONSUMPTION RECORDS WITH RESERVATION IDS")
    print("=" * 50)
    
    consumption_records = []
    for i, record in enumerate(usage_data):
        props = record.get('properties', {})
        additional_info = props.get('additionalInfo', '')
        
        if additional_info:
            try:
                info_data = json.loads(additional_info)
                if 'ReservationOrderId' in info_data:
                    consumption_records.append({
                        'index': i,
                        'instance_name': props.get('instanceName', ''),
                        'reservation_order_id': info_data.get('ReservationOrderId'),
                        'reservation_id': info_data.get('ReservationId', 'N/A'),
                        'service_type': info_data.get('ServiceType', 'N/A'),
                        'consumed_quantity': info_data.get('ConsumedQuantity', 'N/A'),
                        'cost': props.get('costInBillingCurrency', 0),
                        'meter_category': props.get('meterCategory', ''),
                        'charge_type': props.get('chargeType', ''),
                        'full_additional_info': info_data
                    })
            except:
                pass
    
    # Group by reservation order ID
    consumption_by_reservation = defaultdict(list)
    for rec in consumption_records:
        consumption_by_reservation[rec['reservation_order_id']].append(rec)
    
    print(f"Found {len(consumption_records)} consumption records across {len(consumption_by_reservation)} reservations")
    print()
    
    for res_order_id, records in consumption_by_reservation.items():
        print(f"Reservation Order ID: {res_order_id}")
        print(f"  Resources consuming this reservation: {len(records)}")
        
        # Group by instance name
        by_instance = defaultdict(list)
        for rec in records:
            by_instance[rec['instance_name']].append(rec)
        
        for instance_name, instance_records in by_instance.items():
            total_cost = sum(r['cost'] for r in instance_records)
            service_types = set(r['service_type'] for r in instance_records if r['service_type'] != 'N/A')
            consumed_quantities = [r['consumed_quantity'] for r in instance_records if r['consumed_quantity'] != 'N/A']
            
            vm_name = instance_name.split('/')[-1] if '/' in instance_name else instance_name
            print(f"    • {vm_name}: ${total_cost:.2f}")
            if service_types:
                print(f"      Service Types: {', '.join(service_types)}")
            if consumed_quantities:
                total_consumed = sum(float(q) for q in consumed_quantities if isinstance(q, (int, float, str)) and str(q).replace('.','').isdigit())
                print(f"      Total Consumed: {total_consumed:.2f} hours")
        print()
    
    # 4. Specific analysis for the questioned reservation
    print("4. SPECIFIC ANALYSIS: 0a0b55e6-fbb2-4533-9b40-2e6011cbd612")
    print("=" * 50)
    
    target_reservation = "0a0b55e6-fbb2-4533-9b40-2e6011cbd612"
    target_consumption = consumption_by_reservation.get(target_reservation, [])
    
    if not target_consumption:
        print("❌ No consumption records found for this reservation ID!")
        print("   This could mean:")
        print("   1. The reservation exists but isn't being used")
        print("   2. The reservation ID format is different in consumption records")
        print("   3. There's a data processing issue")
    else:
        print(f"✅ Found {len(target_consumption)} consumption records")
        
        # Analyze the VMs by size
        vm_analysis = defaultdict(lambda: {'cost': 0, 'service_types': set(), 'consumed': 0})
        
        for rec in target_consumption:
            vm_name = rec['instance_name'].split('/')[-1] if '/' in rec['instance_name'] else rec['instance_name']
            vm_analysis[vm_name]['cost'] += rec['cost']
            if rec['service_type'] != 'N/A':
                vm_analysis[vm_name]['service_types'].add(rec['service_type'])
            if rec['consumed_quantity'] != 'N/A' and isinstance(rec['consumed_quantity'], (int, float)):
                vm_analysis[vm_name]['consumed'] += float(rec['consumed_quantity'])
        
        print("VM Usage Breakdown:")
        total_cores_used = 0
        
        for vm_name, data in vm_analysis.items():
            service_types = list(data['service_types'])
            cores = 0
            
            # Extract core count from service type
            for stype in service_types:
                if 'D2as_v5' in stype:
                    cores = 2
                elif 'D4as_v5' in stype:
                    cores = 4
                elif 'D8as_v5' in stype:
                    cores = 8
                elif 'D16as_v5' in stype:
                    cores = 16
            
            total_cores_used += cores
            
            print(f"  • {vm_name}")
            print(f"    Cost: ${data['cost']:.2f}")
            print(f"    Service Types: {', '.join(service_types) if service_types else 'N/A'}")
            print(f"    vCPUs: {cores}")
            print(f"    Consumed Hours: {data['consumed']:.2f}")
        
        print(f"\nTotal vCPUs being used: {total_cores_used}")
        print(f"Reserved vCPUs (D8as_v5): 8")
        print(f"Utilization: {(total_cores_used/8)*100:.1f}%")
        
        if total_cores_used > 8:
            print("⚠️  OVER-SUBSCRIBED: Using more cores than reserved!")
        elif total_cores_used < 8:
            print(f"📉 UNDER-UTILIZED: {8-total_cores_used} vCPUs unused")
    
    print()
    
    # 5. Cross-reference: Find all D8as_v5 usage regardless of reservation
    print("5. ALL D8as_v5 USAGE (REGARDLESS OF RESERVATION)")
    print("=" * 50)
    
    all_d8as_usage = []
    for rec in consumption_records:
        if 'D8as_v5' in str(rec['service_type']):
            all_d8as_usage.append(rec)
    
    if all_d8as_usage:
        print(f"Found {len(all_d8as_usage)} D8as_v5 usage records:")
        
        d8as_by_vm = defaultdict(list)
        for rec in all_d8as_usage:
            vm_name = rec['instance_name'].split('/')[-1] if '/' in rec['instance_name'] else rec['instance_name']
            d8as_by_vm[vm_name].append(rec)
        
        for vm_name, records in d8as_by_vm.items():
            reservation_ids = set(r['reservation_order_id'] for r in records)
            total_cost = sum(r['cost'] for r in records)
            total_consumed = sum(float(r['consumed_quantity']) for r in records if isinstance(r['consumed_quantity'], (int, float)))
            
            print(f"  • {vm_name}: ${total_cost:.2f}, {total_consumed:.2f} hours")
            print(f"    Used reservation(s): {', '.join(reservation_ids)}")
    else:
        print("❌ No D8as_v5 usage found in consumption records!")
        print("   Available service types:")
        service_types = set()
        for rec in consumption_records[:50]:  # Sample first 50
            if rec['service_type'] != 'N/A':
                service_types.add(rec['service_type'])
        for stype in sorted(service_types):
            if 'Standard_D' in stype:
                print(f"   - {stype}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 debug_reservations.py /path/to/az_usage_details.json")
        sys.exit(1)
    
    usage_file = sys.argv[1]
    debug_reservations(usage_file)
