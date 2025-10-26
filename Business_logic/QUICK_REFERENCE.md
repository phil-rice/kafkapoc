# Business Logic Quick Reference

## 🎯 One-Page Cheat Sheet

### Event Types
```
EVDAV → Accepted at Depot
EVIMC → In Transit / Mail Centre  
EVGPD → Out for Delivery
ENKDN → Delivered
```

### Product Categories
```
100 → Tracked24        (40% of parcels)
101 → Tracked48        (40% of parcels)
109 → SpecialDelivery09 (10% of parcels)
113 → SpecialDelivery13 (10% of parcels)
```

### Contact Types
```
1 → Email
2 → SMS/Mobile
```

---

## 📋 Notification Rules Matrix

### EVDAV (Accepted at Depot)

| Product | Email | SMS | Notification Code |
|---------|:-----:|:---:|-------------------|
| Tracked24 | ✅ | ❌ | NRARS |
| Tracked48 | ❌ | ❌ | - |
| SpecialDelivery09 | ✅ | ✅ | NRGRS |
| SpecialDelivery13 | ❌ | ✅ | NRJRS |

### EVIMC (In Transit)

**No notifications for any product**

### EVGPD (Out for Delivery)

| Product | Email | SMS | Notification Code |
|---------|:-----:|:---:|-------------------|
| Tracked24 | ✅ | ✅ | NRBRS |
| Tracked48 | ✅ | ✅ | NRERS |
| SpecialDelivery09 | ✅ | ✅ | NRHRS |
| SpecialDelivery13 | ✅ | ✅ | NRKRS |

### ENKDN (Delivered)

| Product | Email | SMS | Notification Code |
|---------|:-----:|:---:|-------------------|
| Tracked24 | ✅ | ❌ | NRCRS |
| Tracked48 | ❌ | ✅ | NRFRS |
| SpecialDelivery09 | ❌ | ✅ | NRIRS |
| SpecialDelivery13 | ✅ | ✅ | NRLRS |

---

## 🔍 Quick Lookup Tables

### By Event Code

```
EVDAV:
  Tracked24 → Email only (NRARS)
  SpecialDelivery09 → Both (NRGRS)
  SpecialDelivery13 → SMS only (NRJRS)

EVIMC:
  (no notifications)

EVGPD:
  All products → Both (NRBRS, NRERS, NRHRS, NRKRS)

ENKDN:
  Tracked24 → Email only (NRCRS)
  Tracked48 → SMS only (NRFRS)
  SpecialDelivery09 → SMS only (NRIRS)
  SpecialDelivery13 → Both (NRLRS)
```

### By Product Category

```
Tracked24:
  EVDAV → Email (NRARS)
  EVGPD → Both (NRBRS)
  ENKDN → Email (NRCRS)

Tracked48:
  EVGPD → Both (NRERS)
  ENKDN → SMS (NRFRS)

SpecialDelivery09:
  EVDAV → Both (NRGRS)
  EVGPD → Both (NRHRS)
  ENKDN → SMS (NRIRS)

SpecialDelivery13:
  EVDAV → SMS (NRJRS)
  EVGPD → Both (NRKRS)
  ENKDN → Both (NRLRS)
```

---

## 🧪 Test Cases

### Test 1: Tracked24 Full Journey
```
Parcel: Tracked24 (productId=100)
Contact: Email + Mobile

Expected Notifications:
✓ EVDAV → 1 notification (email: NRARS)
✓ EVIMC → 0 notifications
✓ EVGPD → 2 notifications (email: NRBRS, sms: NRBRS)
✓ ENKDN → 1 notification (email: NRCRS)

Total: 4 notifications
```

### Test 2: Tracked48 Full Journey
```
Parcel: Tracked48 (productId=101)
Contact: Email + Mobile

Expected Notifications:
✓ EVDAV → 0 notifications
✓ EVIMC → 0 notifications
✓ EVGPD → 2 notifications (email: NRERS, sms: NRERS)
✓ ENKDN → 1 notification (sms: NRFRS)

Total: 3 notifications
```

### Test 3: SpecialDelivery09 Full Journey
```
Parcel: SpecialDelivery09 (productId=109)
Contact: Email + Mobile

Expected Notifications:
✓ EVDAV → 2 notifications (email: NRGRS, sms: NRGRS)
✓ EVIMC → 0 notifications
✓ EVGPD → 2 notifications (email: NRHRS, sms: NRHRS)
✓ ENKDN → 1 notification (sms: NRIRS)

Total: 5 notifications
```

### Test 4: SpecialDelivery13 Full Journey
```
Parcel: SpecialDelivery13 (productId=113)
Contact: Email + Mobile

Expected Notifications:
✓ EVDAV → 1 notification (sms: NRJRS)
✓ EVIMC → 0 notifications
✓ EVGPD → 2 notifications (email: NRKRS, sms: NRKRS)
✓ ENKDN → 2 notifications (email: NRLRS, sms: NRLRS)

Total: 5 notifications
```

### Test 5: No Contact Information
```
Parcel: Any product
Contact: None

Expected Notifications:
✓ All events → 0 notifications

Total: 0 notifications
```

### Test 6: Email Only
```
Parcel: Tracked24
Contact: Email only

Expected Notifications:
✓ EVDAV → 1 notification (email: NRARS)
✓ EVIMC → 0 notifications
✓ EVGPD → 1 notification (email: NRBRS)
✓ ENKDN → 1 notification (email: NRCRS)

Total: 3 notifications
```

### Test 7: Mobile Only
```
Parcel: Tracked48
Contact: Mobile only

Expected Notifications:
✓ EVDAV → 0 notifications
✓ EVIMC → 0 notifications
✓ EVGPD → 1 notification (sms: NRERS)
✓ ENKDN → 1 notification (sms: NRFRS)

Total: 2 notifications
```

---

## 📊 Output Counts

### For 100 Parcels (Perfect Distribution)

**Product Mix:**
- 40 Tracked24
- 40 Tracked48
- 10 SpecialDelivery09
- 10 SpecialDelivery13

**Contact Mix (10% no contact, 10% email, 10% mobile, 70% both):**

**Expected Totals:**
- **Total Events**: 400 (100 parcels × 4 events)
- **Tracking Events**: 400 (all events generate tracking)
- **Billing Events**: 100 (only ENKDN generates billing)
- **Notification Events**: ~210-220 (varies by contact availability)

**Notification Breakdown (Approximate):**
- EVDAV: ~45 notifications
- EVIMC: 0 notifications
- EVGPD: ~150 notifications (most send both email+sms)
- ENKDN: ~55 notifications

---

## 🔧 Debugging Tips

### Check if Notification Should Fire

```
1. What is the Event Code? (EVDAV, EVIMC, EVGPD, ENKDN)
2. What is the Product Category? (Tracked24, Tracked48, SD09, SD13)
3. Look up the combination in the matrix above
4. Check if required contact info exists in auxiliaryData
5. If all match → notification should fire
```

### Common Issues

**No notifications generated:**
- ❌ Contact info missing in auxiliaryData
- ❌ Wrong event code
- ❌ Product category not enriched
- ❌ Event+Product combo not in rules (e.g., EVDAV+Tracked48)

**Wrong notification code:**
- ❌ Product category enrichment failed
- ❌ Event code mismatch

**Duplicate notifications:**
- ❌ Same event processed twice
- ❌ Logic error (should not happen with current CEL)

---

## 🚀 Quick Start Commands

### Generate Test Data
```bash
cd python
python generate_rm_full_dataset_v2.py --parcels 100 --output-dir test_output
```

### Check Generated Summary
```bash
# View notification summary
cat test_output/notifications_summary.csv | head -20

# Count notifications by event
cat test_output/notifications_summary.csv | cut -d',' -f5 | sort | uniq -c

# Count by product category
cat test_output/notifications_summary.csv | cut -d',' -f2 | sort | uniq -c
```

### Validate Notification Counts
```bash
# Total notifications generated
ls test_output/outputs/*.xml | wc -l

# Should match sum of emailNotif + smsNotif in CSV
awk -F',' 'NR>1 {email+=$6; sms+=$7} END {print "Email:",email,"SMS:",sms,"Total:",email+sms}' \
    test_output/notifications_summary.csv
```

---

## 📞 Contact & Support

**For business logic questions:**
- Check: `BUSINESS_LOGIC_README.md` (detailed documentation)
- Reference: `python/generate_rm_full_dataset_v2.py` (source of truth)
- Compare: `test_output/notifications_summary.csv` (expected outputs)

**For CEL syntax questions:**
- See: CEL documentation
- Examples: `cel_business_logic_complete.yaml`

---

## ✅ Quick Validation Checklist

Before deploying:
- [ ] Product ID enrichment works (100→Tracked24, etc.)
- [ ] Contact info extracted from auxiliaryData
- [ ] All 12 notification rules implemented
- [ ] Tracking for all 4 events
- [ ] Billing for ENKDN only
- [ ] Test data validation passes

After deploying:
- [ ] Notification counts match summary CSV
- [ ] No unexpected notification codes
- [ ] All tracking events present
- [ ] Billing events only on delivery

---

**Last Updated:** October 26, 2025  
**Version:** 1.0  
**Status:** Production-ready for POC ✨

