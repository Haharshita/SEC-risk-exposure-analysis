# Market Data Implementation Plan (Phases 4 - 7)

This document outlines the detailed implementation plan for the next stages of the Quantitative Finance pipeline, focusing on stock data retrieval, temporal alignment, risk adjustment, daily event studies, and data cleaning.

## Overview of Next Phases

*   **Phase 4: Monthly Returns & Market Equity**
*   **Phase 5: Fama-French Risk Adjustment**
*   **Phase 6: Daily Returns (Event Study)**
*   **Phase 7: Final Cleaning & Winsorization**

---

### Phase 4: Monthly Returns & Market Equity
**Objective:** Pull monthly stock data from CRSP to calculate Market Equity and set up predictive aligned merging.

**Step 1: Pull CRSP Monthly Data**
*   **Source:** WRDS `crsp.msf` (Monthly Stock File).
*   **Query Fields:** `permno`, `date`, `ret` (return), `prc` (price), and `shrout` (shares outstanding).
*   **Calculated Metrics:** 
    *   **Market Equity (ME):** Calculated as `|prc| * shrout`. The absolute value is used since `prc` can sometimes be recorded as a negative number in CRSP (indicating a bid-ask average rather than a closing price).

**Step 2: Temporal Alignment**
*   **Logic:** Align the SEC risk exposures and financial statements for Year `t` to predict Returns in Year `t+1`. This is a crucial step to avoid look-ahead bias in the regressions.
*   **Actionable:** Shift the CRSP timeline or the exposure timeline during the final pandas `merge` to ensure the correct forecasting structure.

---

### Phase 5: Fama-French Risk Adjustment
**Objective:** Adjust the calculated returns using standard market factors to determine if the "Risk Keywords" provide alpha (excess return).

**Step 1: Download Factors**
*   **Source:** WRDS Fama-French datasets (`ff.factors_monthly`).
*   **Factors to Retrieve:**
    *   `smb` (Small Minus Big - Size Factor)
    *   `hml` (High Minus Low - Value Factor)
    *   `mktrf` (Market Premium)
    *   `rf` (Risk-Free rate)

**Step 2: Calculate Excess Returns**
*   **Formula:** `Excess Return = ret - rf`
*   **Purpose:** This calculation normalizes the raw return against the risk-free rate, allowing subsequent regressions to determine whether the text-based risk exposures yield extra returns over traditional Fama-French risk factors.

---

### Phase 6: Daily Returns (Event Study)
**Objective:** Analyze the immediate market reaction surrounding the SEC 10-K filing dates.

**Step 1: Pull CRSP Daily Data**
*   **Source:** WRDS `crsp.dsf`.
*   **Actionable:** Retrieve daily stock prices around specific "Event Windows" (e.g., the period bounded by `[T-1, T+3]`, where `T` is the exact 10-K filing timestamp).
*   **Analysis:** Determine the cumulative abnormal return (CAR) over the event window to assess market shock due to disclosed risks.

---

### Phase 7: Final Cleaning & Winsorization
**Objective:** Ensure the dataset is robust and immune to extreme outliers before passing it to statistical models.

**Step 1: Winsorize Outliers**
*   **Actionable:** Apply `scipy.stats.mstats.winsorize` to all key continuous variables (including key ratios like ROA, Leverage, and the NLP Exposure frequencies).
*   **Threshold:** Winsorize at the 1% and 99% tails. This clamps extreme skewness that might otherwise dominate OLS regression coefficients.

**Step 2: Drop Invalid Data**
*   **Actionable:** Apply strict drop filters on merged datasets.
*   **Conditions:**
    *   Remove rows where `at` (Assets) is `0` or missing.
    *   Remove rows where `ret` (return) is missing.
*   **Output:** The fully sterilized dataset ready for robust regression and machine learning pipelines.
