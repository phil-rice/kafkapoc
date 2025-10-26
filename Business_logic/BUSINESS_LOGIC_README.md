# Business Logic CEL Files - Complete Documentation

## Overview

This directory contains the complete CEL (Common Expression Language) business logic for the Royal Mail Group Event Processing System POC. The business logic has been **reverse-engineered** from the Python test data generation scripts to replicate the exact notification and tracking rules.

## 📁 Files in this Directory

| File | Purpose | Status |
|------|---------|--------|
| `cel_business_logic_complete.yaml` | **Main file** - Complete business logic with notifications, tracking, and billing | ✅ Ready to use |
| `business_logic_notification.yaml` | Notification-only logic (standalone) | ✅ Reference |
| `business_logic_tracking.yaml` | Tracking-only logic (standalone) | ✅ Reference |
| `cel.yaml` | Old placeholder file with enrichment only | ⚠️ Deprecated |
| `cel.json` | Old placeholder file (JSON format) | ⚠️ Deprecated |

**RECOMMENDED:** Use `cel_business_logic_complete.yaml` as it contains all the logic in one file.

---

## 🎯 Business Logic Rules

### Notification Rules Matrix

The notification logic is based on the `RULES` dictionary from `generate_rm_full_dataset_v2.py`:

| Event Code | Tracked24 | Tracked48 | SpecialDelivery09 | SpecialDelivery13 |
|------------|-----------|-----------|-------------------|-------------------|
| **EVDAV** (Accepted at Depot) | NRA: Email only | - | NRG: Email + SMS | NRJ: SMS only |
| **EVIMC** (In Transit) | - | - | - | - |
| **EVGPD** (Out for Delivery) | NRB: Email + SMS | NRE: Email + SMS | NRH: Email + SMS | NRK: Email + SMS |
| **ENKDN** (Delivered) | NRC: Email only | NRF: SMS only | NRI: SMS only | NRL: Email + SMS |

### Notification Code Format

Each notification gets a `trackedEventCode` = `<prefix>RS`

Examples:
- NRA → **NRARS** (Tracked24 accepted, email notification)
- NRB → **NRBRS** (Tracked24 out for delivery, email/SMS notification)
- NRG → **NRGRS** (SpecialDelivery09 accepted, email/SMS notification)

### Contact Information Sources

The CEL expressions check for contact information in `manualScan.auxiliaryData`:
- **Email**: `auxiliaryData` contains `<data><name>RECIPIENT_EMAILID</name><value>user@example.com</value></data>`
- **Mobile**: `auxiliaryData` contains `<data><name>RECIPIENT_MOBILENO</name><value>07123456789</value></data>`

**Important:** Contact info is only present in the **first scan** (EVDAV) according to the Python script logic:
```python
include_aux = (idx == 1)  # only in first scan like sample
```

### Tracking Rules

**ALL events generate tracking updates:**
- **EVDAV**: Accepted at Depot
- **EVIMC**: In Transit at Mail Centre
- **EVGPD**: Out for Delivery (includes route/walk number, GPS coordinates)
- **ENKDN**: Delivered (includes route/walk number, GPS coordinates)

### Billing Rules

Only **ENKDN (Delivered)** events trigger billing:
- Billing code: `DELIVERY_COMPLETE`
- Includes product category, price paid, and timestamp

---

## 🔍 How the Logic Works

### 1. Product Category Detection

The enrichment layer provides `enrichment.productCategory.productCategory` by looking up the `productId`:

```yaml
# Enrichment (already in place)
lookup:
  "100": "Tracked24"
  "101": "Tracked48"
  "109": "SpecialDelivery09"
  "113": "SpecialDelivery13"
```

### 2. Contact Availability Check

The CEL expressions check if contact information exists:

```cel
manualScan.auxiliaryData.exists(d, d.name == 'RECIPIENT_EMAILID')
manualScan.auxiliaryData.exists(d, d.name == 'RECIPIENT_MOBILENO')
```

### 3. Conditional Notification Generation

Based on product category + event code + contact availability, the logic generates 0, 1, or 2 notifications:

**Example: EVDAV + Tracked24 (email only)**
```cel
enrichment.productCategory.productCategory == 'Tracked24' ? 
  (manualScan.auxiliaryData.exists(d, d.name == 'RECIPIENT_EMAILID') ? 
    [{ 'type': 'notification', 'trackedEventCode': 'NRARS', ... }]
    : []
  )
```

**Example: EVGPD + SpecialDelivery09 (email + sms)**
```cel
enrichment.productCategory.productCategory == 'SpecialDelivery09' ?
  (
    (email exists ? [email notification] : []) +
    (mobile exists ? [sms notification] : [])
  )
```

---

## 📊 Output Formats

### Notification Output

```json
{
  "type": "notification",
  "trackedEventCode": "NRARS",
  "originatingTrackedEventCode": "EVDAV",
  "notificationDestination": "user123456@example.com",
  "notificationDestinationType": 1,
  "notificationRecipientType": "R",
  "eventTimestamp": "2025-10-07T03:00:09+01:00",
  "uniqueItemId": "110999991111001091111",
  "UPUTrackingNumber": "YA123456789GB"
}
```

**notificationDestinationType:**
- `1` = Email
- `2` = SMS/Mobile

### Tracking Output

```json
{
  "type": "tracking",
  "eventCode": "EVGPD",
  "eventName": "Out for Delivery",
  "eventDateTime": "2025-10-07T07:00:09+01:00",
  "uniqueItemId": "110999991111001091111",
  "UPUTrackingNumber": "YA123456789GB",
  "productId": "100",
  "productName": "Tracked24",
  "userId": "User2.test",
  "deviceId": "354176060691111",
  "functionalLocationId": "3333",
  "siteId": "000042",
  "locationName": "City of London DO",
  "postcode": "EC1A1BB",
  "destinationCountry": "GB ",
  "routeOrWalkNumber": "1234567",
  "longitude": -0.127625,
  "latitude": 51.503346,
  "altitude": 0.0
}
```

### Billing Output

```json
{
  "type": "billing",
  "billingCode": "DELIVERY_COMPLETE",
  "eventCode": "ENKDN",
  "uniqueItemId": "110999991111001091111",
  "productId": "100",
  "productCategory": "Tracked24",
  "eventTimestamp": "2025-10-07T09:00:09+01:00",
  "pricePaid": 123
}
```

---

## 🚀 Integration Guide

### Step 1: Deploy the CEL File

Replace the placeholder business logic in your Flink worker configuration:

```bash
# Backup old file
cp worker/flink_worker/src/main/resources/config/prod/parcel/rmg.json \
   worker/flink_worker/src/main/resources/config/prod/parcel/rmg.json.backup

# Deploy new logic (convert YAML to JSON first if needed)
cp Business_logic/cel_business_logic_complete.yaml \
   worker/flink_worker/src/main/resources/config/prod/parcel/rmg.yaml
```

### Step 2: Test with Sample Data

Generate test data using the Python script:

```bash
cd python
python generate_rm_full_dataset_v2.py --parcels 10 --output-dir test_output
```

This generates:
- **40 input XML files** (10 parcels × 4 events each)
- **Up to 30 notification XMLs** (depending on contact availability and rules)
- **notifications_summary.csv** (summary of which notifications were generated)

### Step 3: Publish to Kafka/EventHub

```bash
# Publish input events to input topic
# (Your team handles this step)
```

### Step 4: Verify Outputs

Check the output topics:
- **Notification Topic**: Should contain notification events matching the rules
- **Tracking Topic**: Should contain tracking events for all 4 event types
- **Billing Topic**: Should contain billing events only for ENKDN

---

## 🧪 Testing Scenarios

### Test Case 1: Tracked24 with Email Only

**Input:** 
- Event: EVDAV
- Product ID: 100 (Tracked24)
- auxiliaryData contains: `RECIPIENT_EMAILID`

**Expected Output:**
- 1 notification: NRARS (email)

### Test Case 2: SpecialDelivery09 with Email and Mobile

**Input:**
- Event: EVGPD
- Product ID: 109 (SpecialDelivery09)
- auxiliaryData contains: `RECIPIENT_EMAILID` and `RECIPIENT_MOBILENO`

**Expected Output:**
- 2 notifications: NRHRS (email) + NRHRS (sms)

### Test Case 3: Tracked48 Delivered with No Contact

**Input:**
- Event: ENKDN
- Product ID: 101 (Tracked48)
- auxiliaryData: empty or missing

**Expected Output:**
- 0 notifications (rule requires SMS, but no mobile available)
- 1 tracking event
- 1 billing event

### Test Case 4: EVIMC Event

**Input:**
- Event: EVIMC
- Product ID: 100 (Tracked24)
- Contact info: present

**Expected Output:**
- 0 notifications (EVIMC never generates notifications per rules)
- 1 tracking event

---

## 📋 Validation Checklist

Use this checklist to verify the business logic is working correctly:

- [ ] **EVDAV events**
  - [ ] Tracked24 generates email notification (NRARS) when email exists
  - [ ] SpecialDelivery09 generates both email and SMS when both exist
  - [ ] SpecialDelivery13 generates SMS notification (NRJRS) when mobile exists
  - [ ] All EVDAV events generate tracking updates

- [ ] **EVIMC events**
  - [ ] No notifications generated (regardless of contact info)
  - [ ] All EVIMC events generate tracking updates

- [ ] **EVGPD events**
  - [ ] All product categories generate email + SMS when both exist
  - [ ] Uses correct prefix codes (NRB, NRE, NRH, NRK)
  - [ ] All EVGPD events generate tracking updates with route/GPS info

- [ ] **ENKDN events**
  - [ ] Tracked24 generates email notification only (NRCRS)
  - [ ] Tracked48 generates SMS notification only (NRFRS)
  - [ ] SpecialDelivery09 generates SMS notification only (NRIRS)
  - [ ] SpecialDelivery13 generates both email + SMS (NRLRS)
  - [ ] All ENKDN events generate tracking updates
  - [ ] All ENKDN events generate billing records

---

## 🔧 Troubleshooting

### Issue: No notifications generated

**Check:**
1. Is `enrichment.productCategory.productCategory` populated?
2. Does `manualScan.auxiliaryData` exist and contain contact info?
3. Is the event code + product category combination in the rules matrix?

**Debug:**
```cel
# Add logging to check values
has(enrichment.productCategory.productCategory) ? enrichment.productCategory.productCategory : 'MISSING'
has(manualScan.auxiliaryData) ? 'HAS_AUX' : 'NO_AUX'
```

### Issue: Wrong notification prefix

**Verify:**
- Product category is correctly enriched from productId
- Event code matches exactly (EVDAV, EVIMC, EVGPD, ENKDN)
- Check the rules matrix in this document

### Issue: Duplicate notifications

**Cause:**
- The same notification appears twice (e.g., two NRHRS for the same email)

**Fix:**
- This should not happen with the current logic, as each contact type generates only one notification
- If it does, check if the same event is being processed twice

### Issue: Missing tracking events

**Check:**
- Are all 4 event types configured with tracking logic?
- Is the tracking output router configured correctly?

---

## 🔄 Extending the Logic

### Adding a New Event Type

1. Add a new entry in the `events:` section
2. Define `bizlogic.notification` (if applicable)
3. Define `bizlogic.tracking`
4. Update enrichment lookups if needed

### Adding a New Product Category

1. Update the enrichment lookup to include new productId → category mapping
2. Add rules for the new category in each event's CEL expression
3. Define notification prefix codes for the new category

### Modifying Notification Rules

1. Locate the event + product category combination in the CEL file
2. Modify the prefix code (e.g., change NRA to NRM)
3. Modify the contact type check (email, sms, or both)

Example:
```cel
# Change Tracked24 EVDAV from email-only to email+sms
enrichment.productCategory.productCategory == 'Tracked24' ? 
  (
    (has email ? [email notification with NRARS] : []) +
    (has mobile ? [sms notification with NRARS] : [])
  )
```

---

## 📚 Reference: Python Script Mapping

### Rule Extraction

The notification rules were extracted from:
```python
# python/testdata-generation-scripts/generate_rm_full_dataset_v2.py

RULES = {
    ("EVDAV", "Tracked24"):         ("NRA", True,  False),
    ("EVGPD", "Tracked24"):         ("NRB", True,  True),
    ("ENKDN", "Tracked24"):         ("NRC", True,  False),
    ("EVGPD", "Tracked48"):         ("NRE", True,  True),
    ("ENKDN", "Tracked48"):         ("NRF", False, True),
    ("EVDAV", "SpecialDelivery09"): ("NRG", True,  True),
    ("EVGPD", "SpecialDelivery09"): ("NRH", True,  True),
    ("ENKDN", "SpecialDelivery09"): ("NRI", False, True),
    ("EVDAV", "SpecialDelivery13"): ("NRJ", False, True),
    ("EVGPD", "SpecialDelivery13"): ("NRK", True,  True),
    ("ENKDN", "SpecialDelivery13"): ("NRL", True,  True),
}
```

Where:
- Tuple[0]: Event code
- Tuple[1]: Product category
- Result[0]: Notification prefix (e.g., "NRA")
- Result[1]: Email template exists? (True/False)
- Result[2]: SMS template exists? (True/False)

### Notification Generation Logic

The Python script generates notifications like this:
```python
if tpl_email and has_email:
    seg = build_notification_segment(prefix, ev, email, 1, base_dt)
    segments.append(seg)
    email_notif = True
if tpl_sms and has_mobile:
    seg = build_notification_segment(prefix, ev, mobile, 2, base_dt)
    segments.append(seg)
    sms_notif = True
```

This was translated to CEL as:
```cel
(has email and template exists ? [notification object] : []) +
(has mobile and template exists ? [notification object] : [])
```

---

## 🎓 CEL Language Tips

### Checking if a field exists
```cel
has(manualScan.userId) ? manualScan.userId : ''
```

### Filtering arrays
```cel
manualScan.auxiliaryData.filter(d, d.name == 'RECIPIENT_EMAILID')
```

### Checking array membership
```cel
manualScan.auxiliaryData.exists(d, d.name == 'RECIPIENT_EMAILID')
```

### Concatenating lists
```cel
[item1] + [item2]  # Results in [item1, item2]
```

### Conditional expressions
```cel
condition ? trueValue : falseValue
```

---

## 📞 Support

For questions about this business logic:
1. Check the Python script: `python/testdata-generation-scripts/generate_rm_full_dataset_v2.py`
2. Verify test data outputs: `notifications_summary.csv`
3. Cross-reference with this documentation

---

## ✅ Summary

- ✅ Business logic reverse-engineered from Python test scripts
- ✅ All notification rules implemented exactly as per RULES dictionary
- ✅ Tracking logic covers all 4 event types
- ✅ Billing logic for delivery complete events
- ✅ CEL expressions handle conditional logic for contact availability
- ✅ Output formats match expected API structures
- ✅ Ready for integration testing

**Status**: Production-ready for POC demonstration ✨

