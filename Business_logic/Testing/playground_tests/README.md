# 🧪 CEL Playground Test Suite

## Overview
Generated 64 test cases covering:
- **Events:** EVDAV, EVIMC, EVGPD, ENKDN
- **Products:** Tracked24, Tracked48, SpecialDelivery09, SpecialDelivery13
- **Contact Scenarios:** No contact, Email only, Mobile only, Both

## How to Use

### Quick Test (Single Case)
1. Open https://playcel.undistro.io
2. Pick any test case (e.g., `test_1001_EVDAV_Tracked24_both`)
3. Copy contents of `test_1001_EVDAV_Tracked24_both.cel` → Left panel
4. Copy contents of `test_1001_EVDAV_Tracked24_both_input.json` → Right panel
5. Click "Evaluate"

### Expected Output Format
```json
{
  "out": [
    {"type": "notification", "trackedEventCode": "NRARS", ...},
    {"type": "tracking", "eventCode": "EVDAV", ...}
  ]
}
```

## Test Cases Summary

| Event | Product | Contact | Expected Notifications | Expected Tracking |
|-------|---------|---------|----------------------|------------------|
| EVDAV | Tracked24 | No contact info | None | 1 |
| EVDAV | Tracked24 | Email only | NRARS (Email) | 1 |
| EVDAV | Tracked24 | Mobile only | None | 1 |
| EVDAV | Tracked24 | Both email and mobile | NRARS (Email) | 1 |
| EVDAV | Tracked48 | No contact info | None | 1 |
| EVDAV | Tracked48 | Email only | None | 1 |
| EVDAV | Tracked48 | Mobile only | None | 1 |
| EVDAV | Tracked48 | Both email and mobile | None | 1 |
| EVDAV | SpecialDelivery09 | No contact info | None | 1 |
| EVDAV | SpecialDelivery09 | Email only | NRGRS (Email) | 1 |
| EVDAV | SpecialDelivery09 | Mobile only | NRGRS (SMS) | 1 |
| EVDAV | SpecialDelivery09 | Both email and mobile | NRGRS (Email), NRGRS (SMS) | 1 |
| EVDAV | SpecialDelivery13 | No contact info | None | 1 |
| EVDAV | SpecialDelivery13 | Email only | None | 1 |
| EVDAV | SpecialDelivery13 | Mobile only | NRJRS (SMS) | 1 |
| EVDAV | SpecialDelivery13 | Both email and mobile | NRJRS (SMS) | 1 |
| EVIMC | Tracked24 | No contact info | None | 1 |
| EVIMC | Tracked24 | Email only | None | 1 |
| EVIMC | Tracked24 | Mobile only | None | 1 |
| EVIMC | Tracked24 | Both email and mobile | None | 1 |
| EVIMC | Tracked48 | No contact info | None | 1 |
| EVIMC | Tracked48 | Email only | None | 1 |
| EVIMC | Tracked48 | Mobile only | None | 1 |
| EVIMC | Tracked48 | Both email and mobile | None | 1 |
| EVIMC | SpecialDelivery09 | No contact info | None | 1 |
| EVIMC | SpecialDelivery09 | Email only | None | 1 |
| EVIMC | SpecialDelivery09 | Mobile only | None | 1 |
| EVIMC | SpecialDelivery09 | Both email and mobile | None | 1 |
| EVIMC | SpecialDelivery13 | No contact info | None | 1 |
| EVIMC | SpecialDelivery13 | Email only | None | 1 |
| EVIMC | SpecialDelivery13 | Mobile only | None | 1 |
| EVIMC | SpecialDelivery13 | Both email and mobile | None | 1 |
| EVGPD | Tracked24 | No contact info | None | 1 |
| EVGPD | Tracked24 | Email only | NRBRS (Email) | 1 |
| EVGPD | Tracked24 | Mobile only | NRBRS (SMS) | 1 |
| EVGPD | Tracked24 | Both email and mobile | NRBRS (Email), NRBRS (SMS) | 1 |
| EVGPD | Tracked48 | No contact info | None | 1 |
| EVGPD | Tracked48 | Email only | NRERS (Email) | 1 |
| EVGPD | Tracked48 | Mobile only | NRERS (SMS) | 1 |
| EVGPD | Tracked48 | Both email and mobile | NRERS (Email), NRERS (SMS) | 1 |
| EVGPD | SpecialDelivery09 | No contact info | None | 1 |
| EVGPD | SpecialDelivery09 | Email only | NRHRS (Email) | 1 |
| EVGPD | SpecialDelivery09 | Mobile only | NRHRS (SMS) | 1 |
| EVGPD | SpecialDelivery09 | Both email and mobile | NRHRS (Email), NRHRS (SMS) | 1 |
| EVGPD | SpecialDelivery13 | No contact info | None | 1 |
| EVGPD | SpecialDelivery13 | Email only | NRKRS (Email) | 1 |
| EVGPD | SpecialDelivery13 | Mobile only | NRKRS (SMS) | 1 |
| EVGPD | SpecialDelivery13 | Both email and mobile | NRKRS (Email), NRKRS (SMS) | 1 |
| ENKDN | Tracked24 | No contact info | None | 1 |
| ENKDN | Tracked24 | Email only | NRCRS (Email) | 1 |
| ENKDN | Tracked24 | Mobile only | None | 1 |
| ENKDN | Tracked24 | Both email and mobile | NRCRS (Email) | 1 |
| ENKDN | Tracked48 | No contact info | None | 1 |
| ENKDN | Tracked48 | Email only | None | 1 |
| ENKDN | Tracked48 | Mobile only | NRFRS (SMS) | 1 |
| ENKDN | Tracked48 | Both email and mobile | NRFRS (SMS) | 1 |
| ENKDN | SpecialDelivery09 | No contact info | None | 1 |
| ENKDN | SpecialDelivery09 | Email only | None | 1 |
| ENKDN | SpecialDelivery09 | Mobile only | NRIRS (SMS) | 1 |
| ENKDN | SpecialDelivery09 | Both email and mobile | NRIRS (SMS) | 1 |
| ENKDN | SpecialDelivery13 | No contact info | None | 1 |
| ENKDN | SpecialDelivery13 | Email only | NRLRS (Email) | 1 |
| ENKDN | SpecialDelivery13 | Mobile only | NRLRS (SMS) | 1 |
| ENKDN | SpecialDelivery13 | Both email and mobile | NRLRS (Email), NRLRS (SMS) | 1 |

## Files Structure
- `test_XXXX_EventCode_Product_Scenario.cel` - CEL expression
- `test_XXXX_EventCode_Product_Scenario_input.json` - Test input
- `TEST_SUMMARY.json` - Complete test metadata

## Key Insights
1. **EVIMC has NO notification rules** → Only tracking output
2. **Tracked48 + EVDAV has NO rule** → Only tracking output
3. **Some rules are email-only** (e.g., NRARS, NRCRS)
4. **Some rules are SMS-only** (e.g., NRFRS, NRJRS)
5. **Some rules support both** (e.g., NRBRS, NREBS, NRGRS)

Total test cases: 64
