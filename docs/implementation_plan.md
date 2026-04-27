# SEC 10-K Filing Risk Exposure Analysis — Client Implementation Plan

## Problem Statement

Analyze SEC 10-K filings for **2,800+ companies (CIKs)** across **27 years (2000–2026)** to quantify risk exposure to **40 technology/infrastructure keywords** (data center, AI, cloud computing, etc.). The pipeline covers data download, NLP preprocessing, keyword extraction, normalization, trend analysis, and correlation heatmap generation. The output must be client-ready with full progress tracking.

---

## Phase Breakdown & Detailed Task List

### Phase 1: Data Download (SEC EDGAR 10-K Filings)

| # | Task | Details |
|---|------|---------|
| 1.1 | **Read & deduplicate CIKs** | Load [ciks.txt](file:///C:/Users/Admin/Documents/ciks.txt) → sort → unique list (~2,800 CIKs) |
| 1.2 | **Year-by-year iteration** | Outer loop: years 2000–2026 (27 years) |
| 1.3 | **Batch download (100 CIKs/batch)** | Inner loop: 28 batches per year × 27 years = **756 total batches** |
| 1.4 | **SEC rate-limit compliance** | 150ms sleep per request; ~10 req/sec |
| 1.5 | **Resume logic** | Skip already-processed (CIK, Year) pairs via CSV lookup on restart |
| 1.6 | **Disk cleanup** | Delete raw filings after each CIK to prevent disk overflow |
| 1.7 | **Auto-log time per iteration** | Log start/end time per batch + cumulative ETA to `processing_log.csv` |

### Phase 2: Text Preprocessing (NLP Pipeline)

| # | Task | Details |
|---|------|---------|
| 2.1 | **HTML/table stripping** | Remove `<table>` elements via BeautifulSoup |
| 2.2 | **Contraction expansion** | Map common contractions → full forms |
| 2.3 | **URL & special char removal** | Regex-based cleaning |
| 2.4 | **Tokenization** | NLTK `word_tokenize` |
| 2.5 | **Stopword removal & lemmatization** | NLTK stopwords + WordNet lemmatizer |
| 2.6 | **Inline processing** | Preprocess immediately after download (no separate pass) |

### Phase 3: Keyword Extraction & Feature Engineering

| # | Task | Details |
|---|------|---------|
| 3.1 | **40-keyword regex matching** | Case-insensitive, word-boundary-aware regex for each keyword |
| 3.2 | **Raw count extraction** | `{keyword}_count` columns |
| 3.3 | **Normalization** | `{keyword}_norm = (count / word_count) × 10,000` — mentions per 10K words |
| 3.4 | **Incremental CSV append** | Append each CIK result row to [comprehensive_risk_analysis.csv](file:///C:/Users/Admin/comprehensive_risk_analysis.csv) |
| 3.5 | **Placeholder for missing filings** | Insert `word_count = -1` row for CIKs with no filing found |

### Phase 4: Analysis & Visualization

| # | Task | Details |
|---|------|---------|
| 4.1 | **Top-10 companies per keyword** | Sort by normalized score → export to `top_10_risk_companies.txt` |
| 4.2 | **Yearly trend plots** | One line chart per keyword (avg mentions/10K words vs. year) |
| 4.3 | **Correlation heatmap** | Seaborn heatmap of all 40 normalized keyword columns |
| 4.4 | **Save all plots** | `{keyword}_norm_trend.png` + `correlation_matrix.png` |

### Phase 5: Algorithm Development & Dataset Merging

| # | Task | Details |
|---|------|---------|
| 5.1 | **Start algorithm work with ≥2 years of data** | Begin with whichever 2+ years complete first while remaining years download |
| 5.2 | **Cross-year comparison** | Year-over-year change in keyword exposure per company |
| 5.3 | **Sector clustering** | Group companies by keyword exposure profile (k-means / hierarchical) |
| 5.4 | **Anomaly detection** | Flag companies with sudden spikes in keyword frequency |
| 5.5 | **Multi-year dataset merge** | Merge yearly CSVs into unified panel dataset |
| 5.6 | **Validation on partial data** | Run initial algorithms on 2-year slice to verify correctness before full run |

### Phase 6: Tracking & Client Reporting

| # | Task | Details |
|---|------|---------|
| 6.1 | **Google Sheet creation** | Columns: Problem Statement, Tasks, Comments, Expected Date, Status |
| 6.2 | **Daily status updates** | Auto-update sheet with download/processing progress |
| 6.3 | **Time-per-iteration logging** | Auto-logged via code → surfaced in daily report |
| 6.4 | **ETA estimation** | Based on avg batch time × remaining batches |

---

## Proposed Code Changes

### Logging & Tracking Enhancement

#### [MODIFY] [risk_exposure.py](file:///c:/Users/Admin/Downloads/risk_exposure.py)

1. **Add auto-timing logger** — wrap each batch iteration with `time.time()` start/end; log to `processing_log.csv` with columns: `timestamp, year, batch_start, batch_end, ciks_processed, time_seconds, cumulative_ciks, estimated_remaining_hours`
2. **Add daily summary writer** — at end of each year, append a summary line to `daily_progress.txt` with date, year completed, total CIKs done, avg time/batch

### Google Sheet Tracker

#### [NEW] [create_task_tracker.py](file:///C:/Users/Admin/.gemini/antigravity/scratch/create_task_tracker.py)

Create a Python script that generates a Google Sheet (via `gspread` + service account, or creates a local Excel/CSV that can be uploaded manually) with the following structure:

| Problem Statement | Tasks | Comments | Expected Completion Date | Status |
|---|---|---|---|---|
| SEC 10-K Risk Exposure Analysis | 1.1 Read & deduplicate CIKs | ~2,800 unique CIKs from ciks.txt | 2026-03-27 | ✅ Complete |
| ... | 1.2 Year-by-year iteration setup | 27 years (2000–2026) | 2026-03-27 | ✅ Complete |
| ... | 1.3–1.6 Batch download & cleanup | 756 batches, resume-capable | 2026-04-10 | 🔄 In Progress |
| ... | 2.1–2.6 NLP preprocessing | Inline with download | 2026-04-10 | 🔄 In Progress |
| ... | 3.1–3.5 Keyword extraction | 40 keywords, normalized | 2026-04-10 | 🔄 In Progress |
| ... | 4.1–4.4 Analysis & visualization | Trends + heatmap | 2026-04-15 | ⏳ Pending |
| ... | 5.1–5.6 Algorithm development | Start with 2-yr slice | 2026-04-20 | ⏳ Pending |
| ... | 6.1–6.4 Client reporting | Sheet + daily updates | Ongoing | 🔄 In Progress |

---

## Scalability Strategy

> [!IMPORTANT]
> The code already processes data **incrementally** (append to CSV, skip processed records). To work on next steps while data is still downloading:

1. **Run algorithm development on partial data** — once 2+ years finish, begin Phase 5 tasks in parallel
2. **Uniform timeline** — each dataset (year) follows the same batch→process→append pipeline
3. **No dependency blocking** — analysis scripts read whatever data exists in the CSV at runtime
4. **Idempotent restarts** — resume logic ensures no duplicate processing

---

## Verification Plan

### Automated Verification

1. **Run the modified script for 1 batch** to verify timing logs appear in `processing_log.csv`:
   ```
   python c:\Users\Admin\Downloads\risk_exposure.py
   ```
   Then check `processing_log.csv` exists and has correct columns.

2. **Run the task tracker script** to verify the Excel/Google Sheet is generated:
   ```
   python C:\Users\Admin\.gemini\antigravity\scratch\create_task_tracker.py
   ```
   Then open the generated file and verify all rows match the task breakdown above.

### Manual Verification

1. **Open the generated spreadsheet** and confirm it contains all columns (Problem Statement, Tasks, Comments, Expected Completion Date, Status) with correct task rows
2. **Check `processing_log.csv`** after a few batches run to verify time-per-iteration is being logged correctly
3. **Verify resume logic** by stopping and restarting the script — confirm it skips already-processed CIK/year combos

---

## User Review Required

> [!IMPORTANT]
> **Google Sheets access**: Do you have a Google Cloud service account with Sheets API enabled, or would you prefer I generate a **local Excel file** (`.xlsx`) that you can manually upload/share? The local approach is simpler and doesn't require API credentials.

> [!IMPORTANT]
> **Year range**: The script currently covers 2000–2026 (27 years). The plan above uses this range. Confirm this is correct, or specify a different range.

> [!WARNING]
> **Estimated runtime**: With 2,800 CIKs × 27 years × 0.15s rate limit = ~11,340 seconds (~3.15 hours) minimum just for API calls, plus processing time. Actual wall-clock time is likely **8–15 hours** for a full run. The auto-logging will give precise ETAs after the first few batches.
