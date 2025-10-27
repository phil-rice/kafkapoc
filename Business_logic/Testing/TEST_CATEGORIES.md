# 📊 Test Categories - Quick Reference

## 🎯 Pure Tracking Tests (No Notifications)

### **EVIMC Tests (1017-1032)** ⭐ **RECOMMENDED**
EVIMC has NO notification rules → Pure tracking output

| Test # | File | Product | Contact |
|--------|------|---------|---------|
| 1017 | `test_1017_EVIMC_Tracked24_no_contact_input.json` | Tracked24 | None |
| 1018 | `test_1018_EVIMC_Tracked24_email_only_input.json` | Tracked24 | Email |
| 1019 | `test_1019_EVIMC_Tracked24_mobile_only_input.json` | Tracked24 | Mobile |
| **1020** | **`test_1020_EVIMC_Tracked24_both_input.json`** | **Tracked24** | **Both** ⭐ |
| 1021 | `test_1021_EVIMC_Tracked48_no_contact_input.json` | Tracked48 | None |
| 1022 | `test_1022_EVIMC_Tracked48_email_only_input.json` | Tracked48 | Email |
| 1023 | `test_1023_EVIMC_Tracked48_mobile_only_input.json` | Tracked48 | Mobile |
| 1024 | `test_1024_EVIMC_Tracked48_both_input.json` | Tracked48 | Both |
| 1025 | `test_1025_EVIMC_SpecialDelivery09_no_contact_input.json` | SD09 | None |
| 1026 | `test_1026_EVIMC_SpecialDelivery09_email_only_input.json` | SD09 | Email |
| 1027 | `test_1027_EVIMC_SpecialDelivery09_mobile_only_input.json` | SD09 | Mobile |
| 1028 | `test_1028_EVIMC_SpecialDelivery09_both_input.json` | SD09 | Both |
| 1029 | `test_1029_EVIMC_SpecialDelivery13_no_contact_input.json` | SD13 | None |
| 1030 | `test_1030_EVIMC_SpecialDelivery13_email_only_input.json` | SD13 | Email |
| 1031 | `test_1031_EVIMC_SpecialDelivery13_mobile_only_input.json` | SD13 | Mobile |
| 1032 | `test_1032_EVIMC_SpecialDelivery13_both_input.json` | SD13 | Both |

**All 16 tests output:** `{"out": [{"type": "tracking", ...}]}`

---

## 🔔 Mixed Tests (Tracking + Notifications)

### **High Notification Output**

| Test # | File | Event | Product | Contact | Output |
|--------|------|-------|---------|---------|--------|
| **1012** | `test_1012_EVDAV_SpecialDelivery09_both_input.json` | EVDAV | SD09 | Both | 2 notif + 1 track = **3** |
| **1036** | `test_1036_EVGPD_Tracked24_both_input.json` | EVGPD | Tracked24 | Both | 2 notif + 1 track = **3** |
| **1040** | `test_1040_EVGPD_Tracked48_both_input.json` | EVGPD | Tracked48 | Both | 2 notif + 1 track = **3** |
| **1044** | `test_1044_EVGPD_SpecialDelivery09_both_input.json` | EVGPD | SD09 | Both | 2 notif + 1 track = **3** |
| **1048** | `test_1048_EVGPD_SpecialDelivery13_both_input.json` | EVGPD | SD13 | Both | 2 notif + 1 track = **3** |
| **1064** | `test_1064_ENKDN_SpecialDelivery13_both_input.json` | ENKDN | SD13 | Both | 2 notif + 1 track = **3** |

---

## 📊 Summary by Event Type

### EVDAV (Tests 1001-1016)
- **Pure Tracking:** 12 tests
- **With Notifications:** 4 tests
- **Notification Rules:** 3 (Tracked24-Email, SD09-Both, SD13-SMS)

### EVIMC (Tests 1017-1032) ⭐
- **Pure Tracking:** 16 tests (ALL)
- **With Notifications:** 0 tests
- **Notification Rules:** NONE

### EVGPD (Tests 1033-1048)
- **Pure Tracking:** 4 tests (no contact scenarios)
- **With Notifications:** 12 tests
- **Notification Rules:** 4 (all products with contact)

### ENKDN (Tests 1049-1064)
- **Pure Tracking:** 7 tests
- **With Notifications:** 9 tests
- **Notification Rules:** 4 (varies by product)

---

## 🎯 Recommended Test Sequence

### 1. Pure Tracking First
Start with: **`test_1020_EVIMC_Tracked24_both_input.json`**
- Clean output: 1 tracking event only
- Verify tracking format
- No notifications to confuse

### 2. Single Notification
Then: **`test_1002_EVDAV_Tracked24_email_only_input.json`**
- 1 email notification + 1 tracking
- See both types in output

### 3. Dual Notifications
Finally: **`test_1036_EVGPD_Tracked24_both_input.json`**
- 2 notifications (email + SMS) + 1 tracking
- Full feature demonstration

---

## 📋 Output Format Examples

### Pure Tracking (Test 1020):
```json
{
  "out": [
    {
      "type": "tracking",
      "eventCode": "EVIMC",
      "eventName": "Accepted at Mail Centre",
      "eventDateTime": "2024-10-24T19:35:51+01:00",
      "uniqueItemId": "111968259904001091020",
      "UPUTrackingNumber": "YA084176802GB",
      "productId": "100",
      "productName": "Tracked24",
      "functionalLocationId": "356",
      "siteId": "000175",
      "locationName": "London",
      "postcode": "EC1A1BB",
      "destinationCountry": "GB"
    }
  ]
}
```

### Mixed (Test 1036):
```json
{
  "out": [
    {
      "type": "notification",
      "trackedEventCode": "NRBRS",
      "originatingTrackedEventCode": "EVGPD",
      "notificationDestination": "test1036@example.com",
      "notificationDestinationType": "1",
      ...
    },
    {
      "type": "notification",
      "trackedEventCode": "NRBRS",
      "originatingTrackedEventCode": "EVGPD",
      "notificationDestination": "07088111036",
      "notificationDestinationType": "2",
      ...
    },
    {
      "type": "tracking",
      "eventCode": "EVGPD",
      "eventName": "Out for Delivery",
      ...
    }
  ]
}
```

---

## ✅ Quick Start

1. **For tracking format testing:** Use EVIMC tests (1017-1032)
2. **For notification format testing:** Use EVGPD tests with "both" contact (1036, 1040, 1044, 1048)
3. **For comprehensive testing:** Run all 64 tests

**All tests are in:** `playground_tests/test_*_input.json`

