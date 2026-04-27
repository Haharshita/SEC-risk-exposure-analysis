# Financial and Tech Infrastructure Risk Analysis Pipeline

This repository contains two main scripts used to collect, analyze, and process corporate data: SEC 10-K filings for technology/infrastructure risk exposure, and WRDS Compustat for fundamental financial data.

---

## 1. SEC Risk Exposure Analysis (`risk_exposure.py`)

This script connects to the SEC EDGAR database to download 10-K filings for a list of target companies (CIKs). It uses Natural Language Processing (NLP) to quantify each company's exposure to various technology and infrastructure keywords (e.g., "Data center", "Cloud computing", "Artificial Intelligence").

### Key Features
* **Mass Batch Downloading**: Downloads filings for over 2,800 CIKs across 27 years (2000–2026) in rate-limited batches.
* **Smart Resumption**: Keeps track of processed `(CIK, Year)` pairs in a CSV. If the script is stopped, it automatically resumes exactly where it left off, skipping completed downloads.
* **On-the-fly NLP Preprocessing**: Immediately strips HTML/tables, expands contractions, tokenizes, removes stopwords, and lemmatizes the text using NLTK and BeautifulSoup.
* **Aggressive Disk Cleanup**: Deletes the raw `.html`/`.txt` filings immediately after processing the text to prevent disk space exhaustion.
* **Auto-Timing & Progress Logging**: Computes batch processing times, total elapsed time, and estimates the remaining hours (ETA), appending this to `processing_log.csv`.
* **Output & Visualizations**:
  * Saves extracted term frequencies normalized per 10,000 words to `comprehensive_risk_analysis.csv`.
  * Generates a "Top 10" text report of companies per keyword.
  * Plots year-over-year trend lines for each risk keyword (`{keyword}_trend.png`).
  * Generates a correlation heatmap across all keywords (`correlation_matrix.png`).
* **Auto-updating Tracker**: At the end of the script execution, it updates a specialized Excel file (`SEC_Risk_Exposure_Task_Tracker.xlsx`) with daily progress and task status updates.

### Requirements
```bash
pip install pandas requests beautifulsoup4 nltk sec-edgar-downloader matplotlib seaborn openpyxl
```
*Note: The script will automatically download necessary NLTK corpora (punkt, stopwords, wordnet, omw-1.4) during runtime.*

### Usage
Place your `ciks.txt` file at `C:\Users\Admin\Documents\ciks.txt`, then run:
```bash
python risk_exposure.py
```

---

## 2. Compustat Financial Data Extraction (`compustat.py`)

This script connects to the Wharton Research Data Services (WRDS) database via SQL to extract standard Compustat fundamental financial data, calculate key ratios, and link the corporate identifiers.

### Key Features
* **PostgreSQL Connection**: Patches WRDS SQL execution to use SQLAlchemy `text()` connections and safely connects to WRDS using your username.
* **Targeted Extraction**: Pulls fundamental indicators (Total Assets, Long-Term Debt, Net Income, CapEx, R&D, SG&A, Cash, etc.) for industrial/standardized/consolidated formats strictly for the 2016–2017 period.
* **Financial Ratio Computation**: Calculates derived metrics:
  * `Size` (Log of Assets)
  * `Leverage`
  * `ROA` (Return on Assets)
  * `CapEx_ratio`
  * `RD_intensity` and `SGA_intensity`
  * `Cash_ratio`
  * `BE` (Book Equity approximation)
* **CRSP Linkage**: Queries the CRSP CCM link table (`ccmxpf_linktable`) to join Compustat's `gvkey` with CRSP's `permno` identifiers, ensuring the data date falls within the valid link date range.
* **Outputs**:
  * `compustat_raw.csv`: Raw extracted financials.
  * `compustat_firmyear.csv`: Calculated financial ratios.
  * `ccm_link.csv`: Raw CRSP link table data.
  * `final_linked_financials.csv`: Final merged dataset containing robust financial ratios mapped to standardized CRSP permnos.

### Requirements
```bash
pip install wrds pandas numpy sqlalchemy
```

### Usage
Update the script with your WRDS credentials (default username is `rxg5597`):
```bash
python compustat.py
```
*Note: A `~/.pgpass` file or local credential setup for WRDS should be configured to avoid password prompts.*
