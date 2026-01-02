#!/usr/bin/env python3
"""
Azure Reservations Analysis Report with Estimated Savings
Shows approximate savings when PAYG pricing isn't available
"""

import json
import sys
import re
import argparse
from datetime import datetime, date
from collections import defaultdict

# -------------------- IO & DATE HELPERS -------------------- #

def load_usage_data(file_path):
    """Load and parse the Azure usage JSON file."""
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        return data.get('value', [])
    except Exception as e:
        print(f"Error loading file {file_path}: {e}")
        sys.exit(1)

def parse_iso_date(s):
    """Parse a date/datetime string in common Azure usage payloads to a date()."""
    if not s:
        return None
    # Try strict date first (YYYY-MM-DD)
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except Exception:
        pass
    # Try common ISO datetime variants
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
    except Exception:
        return None

def extract_record_date(props):
    """
    Extract the most appropriate record date from usage record properties.
    Preference order based on common UsageDetails payloads:
    1) 'date' -> single day usage/cost posting
    2) 'usageStart' -> start of usage interval
    3) 'servicePeriodStartDate' -> sometimes present for purchases
    Fallback: 'usageEnd' or 'servicePeriodEndDate'
    """
    for key in ("date", "usageStart", "servicePeriodStartDate"):
        d = parse_iso_date(props.get(key))
        if d:
            return d
    for key in ("usageEnd", "servicePeriodEndDate"):
        d = parse_iso_date(props.get(key))
        if d:
            return d
    return None

def filter_usage_by_date(usage_data, from_date, to_date):
    """
    Filter records to [from_date, to_date] inclusive.
    If no filters are provided, returns the original list.
    Records with no resolvable date are excluded when a filter is applied.
    """
    if not from_date and not to_date:
        return usage_data, 0
    
    filtered = []
    skipped = 0
    for rec in usage_data:
        props = rec.get('properties', {})
        rd = extract_record_date(props)
        if rd is None:
            skipped += 1
            continue
        if from_date and rd < from_date:
            continue
        if to_date and rd > to_date:
            continue
        filtered.append(rec)
    return filtered, skipped

# -------------------- RESERVATION-LEVEL ANALYSIS -------------------- #

def extract_reservation_id(instance_name):
    """Extract reservation ID from instance name."""
    match = re.search(r'reservationOrders/([^/]+)', instance_name)
    return match.group(1) if match else None

def extract_vm_size(product_name):
    """Extract VM size from product name."""
    match = re.search(r'Standard_[A-Za-z0-9_]+', product_name)
    return match.group(0) if match else 'Unknown'

def analyze_reservations_fixed(usage_data):
    """Analyze reservations using consumption records as the source of truth."""
    # Find reservation purchase records visible in this data period
    visible_reservations = {}
    for record in usage_data:
        props = record.get('properties', {})
        instance_name = props.get('instanceName', '')
        if 'reservationOrders' in instance_name and props.get('chargeType') == 'Purchase':
            reservation_id = extract_reservation_id(instance_name)
            if reservation_id:
                visible_reservations[reservation_id] = record
    
    # Find ALL reservation consumption records (including purchases outside this period)
    all_consumption = defaultdict(list)
    all_reservations_seen = set()
    
    for record in usage_data:
        props = record.get('properties', {})
        additional_info = props.get('additionalInfo', '')
        if additional_info:
            try:
                info_data = json.loads(additional_info)
                if 'ReservationOrderId' in info_data:
                    res_id = info_data['ReservationOrderId']
                    all_reservations_seen.add(res_id)
                    consumption_data = {
                        'instance_name': props.get('instanceName', ''),
                        'cost': props.get('costInBillingCurrency', 0),
                        'consumed_quantity': info_data.get('ConsumedQuantity', props.get('quantity', 0)),
                        'service_type': info_data.get('ServiceType', ''),
                        'reservation_id': info_data.get('ReservationId', ''),
                        'meter_category': props.get('meterCategory', ''),
                        'charge_type': props.get('chargeType', ''),
                        'meter_subcategory': props.get('meterSubCategory', ''),
                        'consumed_service': props.get('consumedService', ''),
                        'meter_region': props.get('meterRegion', ''),
                        'effective_price': props.get('effectivePrice', 0),
                        'payg_price': props.get('payGPrice', 0)
                    }
                    all_consumption[res_id].append(consumption_data)
            except Exception:
                pass
    
    return visible_reservations, all_consumption, all_reservations_seen

def get_vm_core_count(service_type):
    """Extract core count from service type (e.g., D8as_v5 -> 8)."""
    if not service_type or service_type == 'Unknown':
        return 0
    match = re.search(r'D(\d+)[a-z]*_v\d+', service_type)
    if match:
        return int(match.group(1))
    return 0

def analyze_reservation_utilization(reservation_id, consumption_records, visible_reservations):
    """Analyze utilization for a specific reservation."""
    reservation_info = visible_reservations.get(reservation_id)
    if reservation_info:
        props = reservation_info.get('properties', {})
        cost = props.get('costInBillingCurrency', 0)
        product = props.get('product', 'Unknown')
        region = props.get('meterRegion', 'Unknown')
        subcategory = props.get('meterSubCategory', 'Unknown')
        start_date = props.get('servicePeriodStartDate', 'Unknown')
        end_date = props.get('servicePeriodEndDate', 'Unknown')
        reserved_size = extract_vm_size(product)
        reserved_cores = get_vm_core_count(reserved_size)
    else:
        # Infer details from consumption records
        cost = "Unknown (purchased outside data period)"
        product = "Unknown"
        region = "Unknown"
        subcategory = "Unknown"
        start_date = "Unknown"
        end_date = "Unknown"
        reserved_size = "Unknown"
        reserved_cores = 0
        
        service_types = {rec['service_type'] for rec in consumption_records if rec['service_type']}
        if service_types:
            max_cores = 0
            for stype in service_types:
                cores = get_vm_core_count(stype)
                if cores > max_cores:
                    max_cores = cores
                    reserved_size = stype
                    reserved_cores = cores
    
    # Aggregate consumption by resource
    resource_usage = defaultdict(lambda: {
        'cost': 0,
        'consumed_hours': 0,
        'service_types': set(),
        'cores_used': 0
    })
    
    for rec in consumption_records:
        instance_name = rec['instance_name']
        vm_name = instance_name.split('/')[-1] if '/' in instance_name else instance_name
        resource_usage[vm_name]['cost'] += rec['cost']
        if isinstance(rec['consumed_quantity'], (int, float)):
            resource_usage[vm_name]['consumed_hours'] += float(rec['consumed_quantity'])
        if rec['service_type']:
            resource_usage[vm_name]['service_types'].add(rec['service_type'])
        cores = get_vm_core_count(rec['service_type'])
        if cores > resource_usage[vm_name]['cores_used']:
            resource_usage[vm_name]['cores_used'] = cores
    
    return {
        'reservation_id': reservation_id,
        'cost': cost,
        'product': product,
        'region': region,
        'subcategory': subcategory,
        'period': f"{start_date} to {end_date}",
        'reserved_size': reserved_size,
        'reserved_cores': reserved_cores,
        'resource_usage': dict(resource_usage),
        'is_visible': reservation_id in visible_reservations
    }

# -------------------- NEW: VM-CENTRIC (COMPUTE-ONLY) COVERAGE -------------------- #

def analyze_vm_coverage(usage_data):
    """
    Per-VM reservation coverage/savings for **compute-only** usage.
    A record is included IFF:
    - meterCategory == "Virtual Machines"
    - consumedService == "Microsoft.Compute"
    - unitOfMeasure contains "Hour" (e.g., "1 Hour", "Hours")
    """
    def is_compute(props):
        mc = (props.get('meterCategory') or '').strip().lower()
        cs = (props.get('consumedService') or '').strip().lower()
        uom = (props.get('unitOfMeasure') or '').strip().lower()
        return (mc == 'virtual machines' and cs == 'microsoft.compute' and 'hour' in uom)
    
    vm = defaultdict(lambda: {
        'total_hours': 0.0,
        'reserved_hours': 0.0,
        'total_cost': 0.0,
        'reserved_cost': 0.0,
        'payg_equiv': 0.0,  # only when payGPrice is known
        'uncovered_payg_cost': 0.0,  # only when payGPrice is known
        'payg_known_hours': 0.0  # hours for which payGPrice was provided
    })
    
    grand = {
        'total_hours': 0.0,
        'reserved_hours': 0.0,
        'total_cost': 0.0,
        'reserved_cost': 0.0,
        'payg_equiv': 0.0,
        'uncovered_payg_cost': 0.0,
        'payg_known_hours': 0.0
    }
    
    for record in usage_data:
        props = record.get('properties', {})
        if not is_compute(props):
            continue
        
        instance_name = props.get('instanceName', '') or '(unknown)'
        vm_name = instance_name.split('/')[-1] if '/' in instance_name else instance_name
        
        additional_info = props.get('additionalInfo', '')
        if additional_info:
            try:
                info = json.loads(additional_info)
            except Exception:
                info = {}
        else:
            info = {}
        
        # Hours
        qty = float(info.get('ConsumedQuantity', props.get('quantity', 0) or 0) or 0)
        
        # Reservation flag
        has_reservation = 'ReservationOrderId' in info
        
        # Cost
        cost = float(props.get('costInBillingCurrency', 0) or 0)
        
        # PAYG unit price (if available)
        payg_price = props.get('payGPrice', None)
        payg_known = isinstance(payg_price, (int, float)) and payg_price > 0
        unit_payg = float(payg_price) if payg_known else None
        
        # Update per-VM bucket
        vm[vm_name]['total_hours'] += qty
        vm[vm_name]['total_cost'] += cost
        
        if has_reservation:
            vm[vm_name]['reserved_hours'] += qty
            vm[vm_name]['reserved_cost'] += cost
        
        if payg_known:
            vm[vm_name]['payg_equiv'] += unit_payg * qty
            vm[vm_name]['payg_known_hours'] += qty
            if not has_reservation:
                vm[vm_name]['uncovered_payg_cost'] += unit_payg * qty
        
        # Update grand totals
        grand['total_hours'] += qty
        grand['total_cost'] += cost
        
        if has_reservation:
            grand['reserved_hours'] += qty
            grand['reserved_cost'] += cost
        
        if payg_known:
            grand['payg_equiv'] += unit_payg * qty
            grand['payg_known_hours'] += qty
            if not has_reservation:
                grand['uncovered_payg_cost'] += unit_payg * qty
    
    return vm, grand

# -------------------- REPORT GENERATION -------------------- #

def generate_fixed_report(usage_file, from_date=None, to_date=None):
    """Generate the corrected reservations report."""
    print("Azure Reservations Analysis Report")
    print("=" * 60)
    print(f"Generated: {datetime.now()}")
    print(f"Source: {usage_file}")
    
    if from_date or to_date:
        fd = from_date.isoformat() if from_date else "—"
        td = to_date.isoformat() if to_date else "—"
        print(f"Filter window: {fd} to {td}")
    print()
    
    usage_data = load_usage_data(usage_file)
    usage_data, skipped = filter_usage_by_date(usage_data, from_date, to_date)
    
    visible_reservations, all_consumption, all_reservations_seen = analyze_reservations_fixed(usage_data)
    
    print("DATA COMPLETENESS CHECK")
    print("=" * 30)
    print(f"Reservations purchased in this period: {len(visible_reservations)}")
    print(f"Total reservations with consumption: {len(all_reservations_seen)}")
    
    hidden_reservations = all_reservations_seen - set(visible_reservations.keys())
    if hidden_reservations:
        print(f"⚠️ Hidden reservations (purchased outside this period): {len(hidden_reservations)}")
        for res_id in hidden_reservations:
            print(f"  • {res_id}")
    
    if skipped:
        print(f"⚠️ Records without resolvable dates skipped due to filter: {skipped}")
    print()
    
    total_visible_cost = 0
    reservation_analyses = []
    
    for reservation_id in sorted(all_reservations_seen):
        consumption_records = all_consumption[reservation_id]
        analysis = analyze_reservation_utilization(reservation_id, consumption_records, visible_reservations)
        reservation_analyses.append(analysis)
        
        print(f"RESERVATION: {reservation_id}")
        print("=" * 50)
        
        if analysis['is_visible'] and isinstance(analysis['cost'], (int, float)):
            print(f"Cost: ${analysis['cost']:.2f}/month")
            total_visible_cost += analysis['cost']
        else:
            print(f"Cost: {analysis['cost']}")
        
        print(f"Product: {analysis['product']}")
        print(f"Region: {analysis['region']}")
        print(f"Category: {analysis['subcategory']}")
        print(f"Period: {analysis['period']}")
        
        if not analysis['is_visible']:
            print("⚠️ This reservation was purchased outside the data period")
        
        print()
        print("✅ STATUS: USED - Found consumption records")
        
        total_cores_used = sum(res['cores_used'] for res in analysis['resource_usage'].values())
        total_consumed_hours = sum(res['consumed_hours'] for res in analysis['resource_usage'].values())
        
        if analysis['reserved_cores'] > 0:
            utilization_pct = (total_cores_used / analysis['reserved_cores']) * 100
            print(f"📊 Core Utilization: {total_cores_used}/{analysis['reserved_cores']} cores ({utilization_pct:.1f}%)")
        
        print(f"📊 Total Consumed Hours: {total_consumed_hours:.2f}")
        
        print("🔍 BENEFICIARY RESOURCES:")
        for vm_name, usage in analysis['resource_usage'].items():
            service_types_str = ', '.join(sorted(usage['service_types']))
            print(f"   • {vm_name}: ${usage['cost']:.2f}")
            print(f"     Service Types: {service_types_str}")
            print(f"     vCPUs: {usage['cores_used']}")
            print(f"     Hours: {usage['consumed_hours']:.2f}")
            
            if analysis['reserved_cores'] > 0 and usage['cores_used'] != analysis['reserved_cores']:
                if usage['cores_used'] < analysis['reserved_cores']:
                    efficiency = (usage['cores_used'] / analysis['reserved_cores']) * 100
                    print(f"     ⚠️  Using {usage['cores_used']} cores of {analysis['reserved_cores']} reserved ({efficiency:.1f}% efficient)")
                elif usage['cores_used'] > analysis['reserved_cores']:
                    print(f"     ⚠️  Needs {usage['cores_used']} cores but reservation only covers {analysis['reserved_cores']}")
        print()
    
    print("OPTIMIZATION ANALYSIS")
    print("=" * 30)
    
    reservation_groups = defaultdict(list)
    for analysis in reservation_analyses:
        if 'VM Instance' in analysis['product'] or analysis['reserved_cores'] > 0:
            key = f"{analysis['reserved_size']}_{analysis['region']}"
            reservation_groups[key].append(analysis)
    
    for group_key, reservations in reservation_groups.items():
        if len(reservations) > 1:
            size, region = group_key.split('_', 1)
            print(f"🔍 Multiple {size} reservations in {region}:")
            
            total_reserved_cores = 0
            total_used_cores = 0
            total_cost = 0
            
            for res in reservations:
                used_cores = sum(usage['cores_used'] for usage in res['resource_usage'].values())
                reserved_cores = res['reserved_cores']
                total_reserved_cores += reserved_cores
                total_used_cores += used_cores
                
                if isinstance(res['cost'], (int, float)):
                    total_cost += res['cost']
                    cost_str = f"${res['cost']:.2f}/month"
                else:
                    cost_str = "Unknown cost"
                
                print(f"   • {res['reservation_id'][:8]}...: {cost_str}")
                print(f"     Using {used_cores}/{reserved_cores} cores")
            
            if total_reserved_cores > 0:
                overall_efficiency = (total_used_cores / total_reserved_cores) * 100
                print(f"   📊 Combined: {total_used_cores}/{total_reserved_cores} cores ({overall_efficiency:.1f}% efficiency)")
                
                if overall_efficiency < 80:
                    print(f"   💡 Consider consolidating or downsizing - low efficiency")
                elif overall_efficiency > 120:
                    print(f"   💡 Consider upsizing reservations - over-subscribed")
            print()
    
    # -------------------- VM-centric coverage output (compute-only) -------------------- #
    vm_stats, grand = analyze_vm_coverage(usage_data)
    
    print("RESERVATION BENEFIT BY VM (COMPUTE ONLY)")
    print("=" * 40)
    
    header = f"{'VM Name':<30} {'Hours':>10} {'Res.Hours':>11} {'%Cov':>6} {'Est.PAYG$*':>11} {'Actual$':>9} {'Savings$*':>10}"
    print(header)
    print("-" * len(header))
    
    def vm_sort_key(item):
        name, st = item
        # sort by uncovered PAYG (when known), then by total hours
        return (st['uncovered_payg_cost'], st['total_hours'])
    
    for name, st in sorted(vm_stats.items(), key=vm_sort_key, reverse=True):
        total_h = st['total_hours']
        res_h = st['reserved_hours']
        actual = st['total_cost']
        pct = (res_h / total_h * 100.0) if total_h > 0 else 0.0
        
        # Only compute Est.PAYG$ and Savings$ when payGPrice was known
        if st['payg_known_hours'] > 0:
            payg_equiv = st['payg_equiv']
            savings = max(0.0, payg_equiv - actual)
            payg_str = f"{payg_equiv:>.2f}"
            savings_str = f"{savings:>.2f}"
        else:
            payg_str = "N/A"
            savings_str = "N/A"
        
        print(f"{name:<30} {total_h:>10.2f} {res_h:>11.2f} {pct:>6.1f} {payg_str:>11} {actual:>9.2f} {savings_str:>10}")
    
    print("\n* Est.PAYG$ and Savings$ shown only where Azure provided payGPrice for the meter.\n")
    
    # -------------------- Overall coverage summary (compute-only) -------------------- #
    total_h = grand['total_hours']
    res_h = grand['reserved_hours']
    coverage_pct = (res_h / total_h * 100.0) if total_h > 0 else 0.0
    uncovered_hours = max(0.0, total_h - res_h)
    
    print("OVERALL RESERVATION COVERAGE (COMPUTE ONLY)")
    print("=" * 40)
    print(f"Covered Hours: {res_h:.2f} / {total_h:.2f} ({coverage_pct:.1f}%)")
    print(f"Uncovered Hours: {uncovered_hours:.2f}")
    print(f"Actual Billed (compute): ${grand['total_cost']:.2f}")
    
    if grand['payg_known_hours'] > 0:
        total_payg_equiv = grand['payg_equiv']
        total_actual = grand['total_cost']
        total_savings = max(0.0, total_payg_equiv - total_actual)
        print(f"\n📊 CALCULATED SAVINGS (where PAYG pricing available)")
        print("-" * 40)
        print(f"Estimated PAYG (if no RIs): ${total_payg_equiv:.2f}")
        print(f"Calculated Savings: ${total_savings:.2f}")
    
    # Always show estimated savings when we have reservation costs
    if total_visible_cost > 0:
        print(f"\n⭐ ESTIMATED TOTAL SAVINGS")
        print("-" * 40)
        
        # Calculate based on visible reservation costs and typical discounts
        min_payg_estimate = total_visible_cost / 0.72  # 28% discount (conservative)
        max_payg_estimate = total_visible_cost / 0.40  # 60% discount (aggressive)
        avg_payg_estimate = (min_payg_estimate + max_payg_estimate) / 2
        
        min_savings = min_payg_estimate - total_visible_cost
        max_savings = max_payg_estimate - total_visible_cost
        avg_savings = avg_payg_estimate - total_visible_cost
        
        print(f"Monthly Reservation Costs: ${total_visible_cost:.2f}")
        print(f"Estimated PAYG equivalent: ${min_payg_estimate:.2f} - ${max_payg_estimate:.2f}")
        print(f"")
        print(f"💰 ESTIMATED MONTHLY SAVINGS: ${min_savings:.2f} - ${max_savings:.2f}")
        print(f"   (Average: ${avg_savings:.2f}/month)")
        print(f"")
        print(f"💰 ESTIMATED ANNUAL SAVINGS: ${min_savings*12:.2f} - ${max_savings*12:.2f}")
        print(f"   (Average: ${avg_savings*12:.2f}/year)")
        
        # Add context about coverage
        if coverage_pct > 0:
            print(f"\nWith {coverage_pct:.1f}% coverage, you're effectively getting:")
            discount_pct_min = (min_savings / min_payg_estimate) * 100
            discount_pct_max = (max_savings / max_payg_estimate) * 100
            print(f"• {discount_pct_min:.0f}%-{discount_pct_max:.0f}% discount on covered compute hours")
        
        if grand['payg_known_hours'] == 0:
            print("\nNote: Azure didn't provide PAYG pricing in this export.")
            print("These estimates are based on typical reservation discounts (28-60%).")
            print("For exact savings, check Azure Portal's Reservation Utilization reports.")
    
    print()
    
    # -------------------- Final summary -------------------- #
    print("SUMMARY")
    print("=" * 20)
    print(f"Total Reservations Found: {len(all_reservations_seen)}")
    print(f"Visible in Data Period: {len(visible_reservations)}")
    print(f"Hidden (Older Purchases): {len(hidden_reservations)}")
    
    if total_visible_cost > 0:
        print(f"Visible Monthly Cost: ${total_visible_cost:.2f}")
        print(f"Visible Annual Cost: ${total_visible_cost * 12:.2f}")
    
    print(f"\n⚠️  IMPORTANT: This analysis is based on consumption data only.")
    print(f"For accurate cost optimization, obtain the complete reservation list from Azure Portal.")
    
    if hidden_reservations:
        print(f"Hidden reservations represent active reservations purchased before the data period.")
    
    print("\n✅ Analysis complete!")

# -------------------- CLI -------------------- #

def valid_yyyy_mm_dd(s):
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError("Date must be in YYYY-MM-DD format")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Analyze Azure reservation utilization from a UsageDetails JSON export."
    )
    parser.add_argument("usage_file", help="Path to az_usage_details.json")
    parser.add_argument("--from", dest="from_date", type=valid_yyyy_mm_dd,
                       help="Start date (YYYY-MM-DD), inclusive")
    parser.add_argument("--to", dest="to_date", type=valid_yyyy_mm_dd,
                       help="End date (YYYY-MM-DD), inclusive")
    
    args = parser.parse_args()
    
    # Validate range order if both provided
    if args.from_date and args.to_date and args.from_date > args.to_date:
        print("Error: --from date cannot be after --to date")
        sys.exit(2)
    
    generate_fixed_report(args.usage_file, from_date=args.from_date, to_date=args.to_date)
