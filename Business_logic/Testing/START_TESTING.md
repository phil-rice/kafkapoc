# 🚀 START TESTING - Quick Guide

## 📁 Your New Structure

```
business_logic_v2/
├── business_logic_notification.yaml    # Notification rules only
├── business_logic_tracking.yaml        # Tracking rules only
├── cel_final.yaml                      # ⭐ Production file (has all 3 sections)
└── Testing/
    ├── cel_playground_all_events.cel   # ⭐ For playground testing
    ├── playground_tests/
    │   ├── test_1001_*_input.json      # Test input 1
    │   ├── test_1002_*_input.json      # Test input 2
    │   └── ... (64 test files)
    ├── TEST_CATEGORIES.md              # Which tests are tracking vs notification
    ├── TRACKING_TEST_GUIDE.md          # Detailed tracking guide
    └── START_TESTING.md                # This file
```

---

## 🎯 Your Goal: Test Tracking Output

You want to:
1. Test the CEL tracking output
2. Understand the format
3. Compare with the tracking API format you showed

---

## ⚠️ CRITICAL: Format Clarification

### What You Showed (Tracking API Response):
```json
{
  "parcel": {
    "requestId": "...",
    "details": {
      "events": [...]  // Multiple events aggregated
    }
  }
}
```
**This is the FINAL API response** after multiple events are collected.

### What CEL Outputs (Per Event):
```json
{
  "out": [
    {
      "type": "tracking",
      "eventCode": "EVDAV",  // Single event
      ...
    }
  ]
}
```
**This is the INPUT to the tracking service** - one event at a time.

### The Flow:
```
CEL Output (Event 1: EVDAV) → Tracking Service → Store in DB
CEL Output (Event 2: EVIMC) → Tracking Service → Store in DB
CEL Output (Event 3: EVGPD) → Tracking Service → Store in DB
                                                      ↓
User calls Tracking API → Query DB → Aggregate all events → Return
```

**Your CEL is correct!** It outputs individual events. The Tracking API aggregates them.

---

## 🧪 Test Now (3 Steps)

### Step 1: Open Playground
🌐 **https://playcel.undistro.io**

### Step 2: Copy CEL (Left Panel) - ONCE
Copy the entire contents of:
**`Testing/cel_playground_all_events.cel`**

Keep this in the left panel for ALL tests!

### Step 3: Test Pure Tracking (Right Panel)

#### Recommended Test: EVIMC (No Notifications)
Copy: **`Testing/playground_tests/test_1020_EVIMC_Tracked24_both_input.json`**

**Click "Evaluate"**

#### Expected Output:
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

**Verify:**
- ✅ Single tracking event in output
- ✅ Has all required fields
- ✅ NO notifications (EVIMC has no notif rules)

---

## 📊 Which Tests to Use

### For Pure Tracking (No Notifications):
**Use EVIMC tests (1017-1032)** - All 16 output only tracking

**Best:** `test_1020_EVIMC_Tracked24_both_input.json`

### For Tracking + Notifications:
**Use EVGPD tests with "both" contact:**
- `test_1036_EVGPD_Tracked24_both_input.json` → 2 notif + 1 track
- `test_1044_EVGPD_SpecialDelivery09_both_input.json` → 2 notif + 1 track

**See `TEST_CATEGORIES.md` for complete list**

---

## 📋 Tracking Output Fields

### Fields in CEL Output:
```json
{
  "type": "tracking",           // Always "tracking"
  "eventCode": "EVIMC",          // Event type
  "eventName": "...",            // Human-readable name
  "eventDateTime": "...",        // ISO timestamp
  "uniqueItemId": "...",         // Parcel ID (key field)
  "UPUTrackingNumber": "...",    // Barcode
  "productId": "100",            // Product code
  "productName": "Tracked24",    // Product name
  "functionalLocationId": "356", // Location ID
  "siteId": "000175",            // Site ID
  "locationName": "London",      // Enriched location
  "postcode": "EC1A1BB",         // Destination postcode
  "destinationCountry": "GB"     // Destination country
}
```

### Additional Fields (EVGPD, ENKDN only):
```json
{
  "userId": "user123",           // Who scanned
  "deviceId": "device456",       // Device used
  "routeOrWalkNumber": "R01",    // Route (EVGPD only)
  "longitude": "-0.127625",      // GPS (EVGPD only)
  "latitude": "51.503346",       // GPS (EVGPD only)
  "altitude": "0.0"              // GPS (EVGPD only)
}
```

---

## 🔄 How Tracking Service Uses This

### CEL Output → Tracking Service:
```json
{"type": "tracking", "eventCode": "EVIMC", "uniqueItemId": "123", ...}
```

### Tracking Service Logic:
1. Reads from Kafka topic
2. Extracts `uniqueItemId`: "123"
3. Stores event in Cosmos DB under key "123"
4. Appends to existing events for that parcel

### Tracking API Query:
```
GET /tracking/123
```

### Tracking API Response (Aggregated):
```json
{
  "parcel": {
    "uniqueItemId": "123",
    "events": [
      {"eventCode": "EVDAV", ...},  // From earlier CEL output
      {"eventCode": "EVIMC", ...},  // From current CEL output
      {"eventCode": "EVGPD", ...}   // From future CEL output
    ]
  }
}
```

---

## ✅ Your CEL is Correct!

**What CEL does:** Outputs individual events with all required fields ✅

**What Tracking Service does:** Collects and aggregates events per parcel ✅

**What Tracking API does:** Returns aggregated view to users ✅

---

## 🎯 Test Sequence

### 1. Pure Tracking (EVIMC)
```
File: test_1020_EVIMC_Tracked24_both_input.json
Expected: 1 tracking event
Purpose: Verify tracking format
```

### 2. Single Event (EVDAV + Email)
```
File: test_1002_EVDAV_Tracked24_email_only_input.json
Expected: 1 notification + 1 tracking
Purpose: See both types
```

### 3. Multiple Events (EVGPD + Both)
```
File: test_1036_EVGPD_Tracked24_both_input.json
Expected: 2 notifications + 1 tracking
Purpose: Full demonstration
```

### 4. All Event Types
Test one from each: EVDAV, EVIMC, EVGPD, ENKDN
- Shows different event names
- Shows optional fields (userId, deviceId, GPS)

---

## 📚 Documentation Files

- **`TEST_CATEGORIES.md`** - Which tests are tracking vs notification
- **`TRACKING_TEST_GUIDE.md`** - Detailed tracking explanation
- **`START_TESTING.md`** - This file

---

## 🚀 Ready to Test!

**Open:** https://playcel.undistro.io

**Left Panel:** `Testing/cel_playground_all_events.cel`

**Right Panel:** `Testing/playground_tests/test_1020_EVIMC_Tracked24_both_input.json`

**Click:** "Evaluate"

**Verify:** You get a single tracking event with all fields ✅

---

**Note:** If scripts need to be run again, they're in the deleted files. Let me know if you need them regenerated with updated paths!

