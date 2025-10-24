python generate_rm_full_dataset_v2.py --parcels 100 --output-dir out --batch-size 200


python generate_rm_full_dataset_v2.py --parcels 10000000 --output-dir out_10M --batch-size 100000 --seed 42

| Artifact | Count        | Files per zip        | Notes                  |
| -------- | ------------ | -------------------- | ---------------------- |
| Inputs   | 40M XMLs     | 100k/zip → ~400 zips | 4 events per parcel    |
| Outputs  | ~15–20M XMLs | 100k/zip             | depends on contact mix |
| Combined | 10M XMLs     | 100k/zip → ~100 z    |                        |


Here’s a clean, concise **README.md** you can drop directly beside your `generate_rm_full_dataset.py` script 👇

---

# 📦 Royal Mail Dataset Generator

This utility generates realistic **Royal Mail–style parcel scan and notification data** for testing large-scale ingestion, transformation, and analytics pipelines.

It produces:

* **Input XMLs** – 4 scans per parcel (e.g., EVDAV, EVIMC, EVGPD, ENKDN)
* **Output XMLs** – notification events (email/SMS) as per notification rules
* **Combined XMLs** – input & output events merged into `<test>` files
* **CSV summaries** – postcode data and notification summaries

---

## 📁 Output Structure

After generation, the output directory will contain:

```
out/
├── inputs/                     # input scan XMLs in zip batches
│   ├── mp_inputs_0001.zip
├── outputs/                    # notification XMLs in zip batches
│   ├── mp_notifications_0001.zip
├── combined/                   # merged input-output test XMLs
│   ├── test_combined_0001.zip
├── postcode.csv                # address + postcode master data
├── postcode_suffix.csv         # postcode + DPS suffix variants
└── notifications_summary.csv   # per-event notification coverage
```

---

## ⚙️ Basic Usage

### 1️⃣ Generate a small sample (100 parcels)

This produces a quick test dataset with all output types:

```bash
python generate_rm_full_dataset.py \
  --parcels 100 \
  --output-dir out_sample \
  --batch-size 200 \
  --seed 42
```

💡 This creates roughly:

* 400 input XMLs (4 per parcel)
* ~250–300 notification XMLs (depending on contact mix)
* 100 combined XMLs
* CSVs in `out_sample/`

---

## 🚀 Generate a large dataset

### 2️⃣ Generate **10 million parcels**

This simulates **10 million parcels × 4 input scans = 40M input XMLs**, plus corresponding outputs and combined data.

```bash
python generate_rm_full_dataset.py \
  --parcels 10000000 \
  --output-dir out_10M_parcels \
  --batch-size 100000 \
  --seed 42
```

💾 Approximate scale:

* **Inputs:** 40 million XMLs (zipped in 100k-file batches)
* **Outputs:** ~25–30 million XMLs (depends on notification mix)
* **Combined files:** 10 million `<test>` XMLs
* **Disk space:** ~400–600 GB (depending on compression)

---

### 3️⃣ Generate **10 million total events**

If you want *10 million total events (inputs)*, i.e. 2.5 M parcels × 4 scans each:

```bash
python generate_rm_full_dataset.py \
  --parcels 2500000 \
  --output-dir out_10M_events \
  --batch-size 100000 \
  --seed 42
```

💾 Approximate scale:

* **Inputs:** ~10 M XMLs
* **Outputs:** ~6–7 M XMLs
* **Combined:** 2.5 M `<test>` XMLs
* **Disk space:** ~100 GB (compressed)

---

## 🧠 Optional Arguments

| Argument          | Description                                           | Default                |
| ----------------- | ----------------------------------------------------- | ---------------------- |
| `--parcels`       | Number of parcels to generate                         | 100                    |
| `--output-dir`    | Destination directory for output                      | `out_full`             |
| `--batch-size`    | Number of XML files per zip                           | 100000                 |
| `--postcode-file` | Path to custom postcode list (one per line, optional) | uses built-in defaults |
| `--seed`          | Random seed for reproducibility                       | 42                     |
| `--start-date`    | Earliest generation date (YYYY-MM-DD)                 | today − 365 days       |
| `--end-date`      | Latest generation date (YYYY-MM-DD)                   | today                  |

---

## 📋 Outputs Overview

| File                        | Description                                                     |
| --------------------------- | --------------------------------------------------------------- |
| `postcode.csv`              | Base postcode + address metadata                                |
| `postcode_suffix.csv`       | Per-postcode DPS suffixes                                       |
| `notifications_summary.csv` | CSV summary of which parcels generated which notification types |
| `inputs/*.zip`              | 4 XMLs per parcel (raw scans)                                   |
| `outputs/*.zip`             | Notification XMLs (email/SMS)                                   |
| `combined/*.zip`            | Combined input-output `<test>` XMLs                             |
