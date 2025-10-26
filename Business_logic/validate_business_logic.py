"""
Business Logic Validation Script

This script validates that the CEL business logic produces the correct outputs
by comparing against the expected notification rules from the Python test data generator.

Usage:
    python validate_business_logic.py [--summary-csv path/to/notifications_summary.csv]

Author: AI Assistant
Date: October 26, 2025
"""

import argparse
import csv
import sys
from collections import defaultdict

# Notification rules from generate_rm_full_dataset_v2.py
RULES = {
    ("EVDAV", "Tracked24"): ("NRA", True, False),  # email only
    ("EVGPD", "Tracked24"): ("NRB", True, True),   # email + sms
    ("ENKDN", "Tracked24"): ("NRC", True, False),  # email only
    ("EVGPD", "Tracked48"): ("NRE", True, True),   # email + sms
    ("ENKDN", "Tracked48"): ("NRF", False, True),  # sms only
    ("EVDAV", "SpecialDelivery09"): ("NRG", True, True),  # email + sms
    ("EVGPD", "SpecialDelivery09"): ("NRH", True, True),  # email + sms
    ("ENKDN", "SpecialDelivery09"): ("NRI", False, True), # sms only
    ("EVDAV", "SpecialDelivery13"): ("NRJ", False, True), # sms only
    ("EVGPD", "SpecialDelivery13"): ("NRK", True, True),  # email + sms
    ("ENKDN", "SpecialDelivery13"): ("NRL", True, True),  # email + sms
}


def validate_notification_logic(summary_csv_path):
    """
    Validates that notifications in the summary CSV match the expected rules.
    
    Returns:
        (bool, list): (all_valid, list_of_errors)
    """
    errors = []
    all_valid = True
    stats = defaultdict(int)
    
    try:
        with open(summary_csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row_num, row in enumerate(reader, start=2):
                unique_id = row['uniqueItemId']
                category = row['productCategory']
                has_email = bool(int(row['hasEmail']))
                has_mobile = bool(int(row['hasMobile']))
                event = row['event']
                email_notif = bool(int(row['emailNotif']))
                sms_notif = bool(int(row['smsNotif']))
                
                # Check if this event+category combo should generate notifications
                key = (event, category)
                if key in RULES:
                    prefix, tpl_email, tpl_sms = RULES[key]
                    
                    # Expected notifications
                    expected_email = tpl_email and has_email
                    expected_sms = tpl_sms and has_mobile
                    
                    # Validate
                    if email_notif != expected_email:
                        errors.append(
                            f"Row {row_num}: {unique_id} - {event}/{category} - "
                            f"Email notification mismatch: got {email_notif}, expected {expected_email}"
                        )
                        all_valid = False
                    
                    if sms_notif != expected_sms:
                        errors.append(
                            f"Row {row_num}: {unique_id} - {event}/{category} - "
                            f"SMS notification mismatch: got {sms_notif}, expected {expected_sms}"
                        )
                        all_valid = False
                    
                    # Track stats
                    stats['total_rows'] += 1
                    if email_notif:
                        stats['email_notifications'] += 1
                    if sms_notif:
                        stats['sms_notifications'] += 1
                    stats[f'{event}_{category}'] += 1
                else:
                    # Event+category combo not in rules - should have no notifications
                    if email_notif or sms_notif:
                        errors.append(
                            f"Row {row_num}: {unique_id} - {event}/{category} - "
                            f"Unexpected notification (not in rules)"
                        )
                        all_valid = False
    
    except FileNotFoundError:
        print(f"❌ Error: File not found: {summary_csv_path}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
        sys.exit(1)
    
    return all_valid, errors, stats


def print_statistics(stats):
    """Print validation statistics."""
    print("\n📊 Validation Statistics")
    print("=" * 60)
    print(f"Total rows validated: {stats['total_rows']}")
    print(f"Email notifications: {stats['email_notifications']}")
    print(f"SMS notifications: {stats['sms_notifications']}")
    print(f"Total notifications: {stats['email_notifications'] + stats['sms_notifications']}")
    
    print("\n📋 Breakdown by Event + Product Category:")
    for key in sorted(stats.keys()):
        if key.startswith('EV') or key.startswith('EN'):
            print(f"  {key}: {stats[key]}")


def print_rules_summary():
    """Print a summary of the notification rules."""
    print("\n📖 Notification Rules Summary")
    print("=" * 60)
    
    events = sorted(set(event for event, _ in RULES.keys()))
    categories = sorted(set(category for _, category in RULES.keys()))
    
    for event in events:
        print(f"\n{event}:")
        for category in categories:
            key = (event, category)
            if key in RULES:
                prefix, tpl_email, tpl_sms = RULES[key]
                notification_code = f"{prefix}RS"
                
                if tpl_email and tpl_sms:
                    contact = "Email + SMS"
                elif tpl_email:
                    contact = "Email only"
                elif tpl_sms:
                    contact = "SMS only"
                else:
                    contact = "None"
                
                print(f"  {category:20s} → {notification_code} ({contact})")


def main():
    parser = argparse.ArgumentParser(
        description="Validate business logic against notification rules"
    )
    parser.add_argument(
        '--summary-csv',
        type=str,
        default='../python/test_output/notifications_summary.csv',
        help='Path to notifications_summary.csv (default: ../python/test_output/notifications_summary.csv)'
    )
    parser.add_argument(
        '--show-rules',
        action='store_true',
        help='Show notification rules summary and exit'
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("🔍 Business Logic Validation")
    print("=" * 60)
    
    if args.show_rules:
        print_rules_summary()
        return
    
    print(f"\nValidating: {args.summary_csv}")
    
    all_valid, errors, stats = validate_notification_logic(args.summary_csv)
    
    print_statistics(stats)
    
    if all_valid:
        print("\n" + "=" * 60)
        print("✅ VALIDATION PASSED")
        print("=" * 60)
        print("\nAll notifications match the expected business logic rules!")
        print("The CEL configuration is correct and ready for deployment.")
    else:
        print("\n" + "=" * 60)
        print("❌ VALIDATION FAILED")
        print("=" * 60)
        print(f"\nFound {len(errors)} error(s):\n")
        for error in errors[:10]:  # Show first 10 errors
            print(f"  • {error}")
        if len(errors) > 10:
            print(f"\n  ... and {len(errors) - 10} more error(s)")
        
        sys.exit(1)


if __name__ == "__main__":
    main()

