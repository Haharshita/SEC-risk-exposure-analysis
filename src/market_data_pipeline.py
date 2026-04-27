import os
import getpass
import pandas as pd
import numpy as np
from datetime import timedelta
from scipy.stats.mstats import winsorize
import wrds

def setup_wrds_pgpass(username, password):
    """
    Creates the PostgreSQL password file so WRDS library can authenticate
    without hanging on console prompts. Windows expects this at %APPDATA%/postgresql/pgpass.conf
    """
    try:
        appdata = os.environ.get('APPDATA')
        if not appdata:
            appdata = os.path.expanduser('~')
        
        pgdir = os.path.join(appdata, 'postgresql')
        os.makedirs(pgdir, exist_ok=True)
        pgpass_file = os.path.join(pgdir, 'pgpass.conf')
        
        with open(pgpass_file, 'w') as f:
            f.write(f"wrds-pgdata.wharton.upenn.edu:9737:wrds:{username}:{password}\n")
            
        # Set environment variable just in case
        os.environ['PGPASSFILE'] = pgpass_file
        print(f"[+] WRDS Authentication configured securely at {pgpass_file}")
    except Exception as e:
        print(f"[-] Could not structure pgpass file: {e}")

def main():
    print("="*50)
    print(" Market Data & Risk Pipeline (Phases 4 - 7) ")
    print("="*50)

    # 1. Credentials setup for WRDS
    username = input("Please enter your WRDS Username: ").strip()
    password = "f.Tu*Piz3G!AQ-&"
    setup_wrds_pgpass(username, password)
    
    print("\n[+] Connecting to WRDS Database...")
    try:
        db = wrds.Connection(wrds_username=username)
    except Exception as e:
        print(f"[-] WRDS connection failed: {e}")
        return

    # 2. Loading Core Structured Financials & NLP data
    base_file = r"C:\Users\Admin\Downloads\New folder\final_linked_financials.csv"
    if not os.path.exists(base_file):
        print(f"[-] Base dataset missing: {base_file}")
        return
        
    print(f"[+] Loading {base_file}...")
    df_main = pd.read_csv(base_file)
    df_main['datadate'] = pd.to_datetime(df_main['datadate'])
    
    nlp_file = r"C:\Users\Admin\Downloads\New folder\comprehensive_risk_analysis.csv"
    if os.path.exists(nlp_file):
        print(f"[+] Loading NLP Risk data from {nlp_file}...")
        df_nlp = pd.read_csv(nlp_file)
        # Clean cik to string for strict merging
        df_nlp['cik'] = df_nlp['cik'].astype(str).str.lstrip('0')
        df_main['cik_str'] = df_main['cik'].astype(str).str.lstrip('0')
        # Merge the NLP risk components into our financial base on CIK and Year
        df_main = pd.merge(df_main, df_nlp, left_on=['cik_str', 'fyear'], right_on=['cik', 'Year'], how='inner')
    else:
        print(f"[-] WARNING: NLP dataset missing at {nlp_file}. Models will lack keyword vectors!")
    
    # We strip permutations of CIKs to query the EDGAR DB effectively
    permnos = tuple(df_main['permno'].dropna().unique().astype(int).tolist())
    ciks = tuple(df_main['cik'].dropna().unique().astype(str).tolist())
    
    # ---------------------------------------------------------
    # PHASE 6 FIX: FETCHING SEC FILING TIMESTAMPS
    # ---------------------------------------------------------
    print("\n[PHASE 6 FIX] Fetching SEC Exact 10-K Filing Dates from WRDS...")
    ciks_padded = tuple([str(c).zfill(10) for c in ciks])
    
    # We query the wrdssec.filings DB to get the specific day they filed the 10-K 
    # to act as our `T` in the daily event study bounds.
    query_sec = f"""
        SELECT cik, fdate as filing_date, form
        FROM wrdssec.filings
        WHERE cik IN {ciks_padded}
        AND form IN ('10-K', '10-K405', '10-KSB')
    """
    try:
        sec_filings = db.raw_sql(query_sec)
        sec_filings['filing_date'] = pd.to_datetime(sec_filings['filing_date'])
        sec_filings['cik'] = sec_filings['cik'].astype(str).str.lstrip('0')
        sec_filings['year'] = sec_filings['filing_date'].dt.year
        
        # A 10-K filed in Year T corresponds to the fiscal results of Year T-1
        sec_filings['fyear'] = sec_filings['year'] - 1
        
        # Sorting and dropping potential duplicated amendment filings, keeping original
        sec_filings = sec_filings.sort_values(by=['cik', 'fyear', 'filing_date']).drop_duplicates(subset=['cik', 'fyear'], keep='first')
        
        if 'cik_str' not in df_main.columns:
            df_main['cik_str'] = df_main['cik'].astype(str).str.lstrip('0')
        df_main = pd.merge(df_main, sec_filings[['cik', 'fyear', 'filing_date']], left_on=['cik_str', 'fyear'], right_on=['cik', 'fyear'], how='left')
        print(f"   Integrated {sec_filings.shape[0]} historical filing timestamps.")
    except Exception as e:
        print(f"[-] Could not access wrdssec.filings (Requires specialized subscription): {e}")
        print("    Applying Fallback Method: Estimating Filing as datadate + 90 days.")
        df_main['filing_date'] = df_main['datadate'] + pd.to_timedelta(90, unit='d')

    # ---------------------------------------------------------
    # PHASE 4: MONTHLY RETURNS & MARKET EQUITY (TEMPORAL SHIFT)
    # ---------------------------------------------------------
    print("\n[PHASE 4] Pulling CRSP Monthly Return (crsp.msf) & Market Equity...")
    query_msf = f"""
        SELECT permno, date, ret, prc, shrout
        FROM crsp.msf
        WHERE permno IN {permnos}
        AND date >= '1995-01-01'
    """
    crsp_msf = db.raw_sql(query_msf)
    crsp_msf['date'] = pd.to_datetime(crsp_msf['date'])
    crsp_msf['year'] = crsp_msf['date'].dt.year
    crsp_msf['month'] = crsp_msf['date'].dt.month
    
    # Calculate Absolute ME
    crsp_msf['ME'] = crsp_msf['prc'].abs() * crsp_msf['shrout']

    # ---------------------------------------------------------
    # PHASE 5: FAMA-FRENCH RISK FACTORS
    # ---------------------------------------------------------
    print("\n[PHASE 5] Retrieving Fama-French Risk Factors (ff.factors_monthly) & Computing Excess Returns...")
    query_ff = """
        SELECT dateff as date, smb, hml, mktrf, rf
        FROM ff.factors_monthly
        WHERE dateff >= '1995-01-01'
    """
    ff_factors = db.raw_sql(query_ff)
    ff_factors['date'] = pd.to_datetime(ff_factors['date'])
    
    # Merge on Year-Month frequency
    crsp_msf['date_month'] = crsp_msf['date'].dt.to_period('M')
    ff_factors['date_month'] = ff_factors['date'].dt.to_period('M')
    crsp_msf = pd.merge(crsp_msf, ff_factors, on='date_month', how='inner')
    
    # Core Alpha Variable
    crsp_msf['Excess_Return'] = crsp_msf['ret'] - crsp_msf['rf']

    print("   Aggregating Temporal Output T -> T+1...")
    # For every datadate in df_main, the structural return is year+1
    crsp_msf['fyear'] = crsp_msf['year']
    crsp_annual = crsp_msf.groupby(['permno', 'fyear']).agg({
        'Excess_Return': lambda x: np.prod(1 + x) - 1, # Annual Geometric compounding
        'ME': 'last', # EOY Market Cap
        'ret': lambda x: np.prod(1 + x) - 1
    }).reset_index()
    
    # Temporally shift the structural data backwards by 1 year conceptually
    # e.g., Returns from 2020 are merged onto SEC Risk statements from 2019
    crsp_annual['fyear_prev'] = crsp_annual['fyear'] - 1
    
    df_aligned = pd.merge(df_main, crsp_annual, left_on=['permno', 'fyear'], right_on=['permno', 'fyear_prev'], how='left', suffixes=('', '_y'))
    
    # ---------------------------------------------------------
    # PHASE 6 CONTINUED: DAILY EVENT STUDY CAR CALCULATION
    # ---------------------------------------------------------
    print("\n[PHASE 6] Interrogating CRSP Daily File (crsp.dsf) for [T-1, T+3] Event Windows...")
    valid_events = df_aligned.dropna(subset=['filing_date', 'permno'])
    
    if not valid_events.empty:
        # Optimization: To avoid requesting multi-million line datasets, we bound globally
        min_date = valid_events['filing_date'].min() - pd.to_timedelta(10, unit='d')
        max_date = valid_events['filing_date'].max() + pd.to_timedelta(10, unit='d')
        
        query_dsf = f"""
            SELECT permno, date, ret
            FROM crsp.dsf
            WHERE permno IN {permnos}
            AND date >= '{min_date.strftime('%Y-%m-%d')}'
            AND date <= '{max_date.strftime('%Y-%m-%d')}'
        """
        crsp_dsf = db.raw_sql(query_dsf)
        crsp_dsf['date'] = pd.to_datetime(crsp_dsf['date'])
        crsp_dsf = crsp_dsf.dropna(subset=['ret'])
        
        print("   Cross-computing Cumulative Abnormal Returns (CAR)...")
        # Build memory lookup for extreme speed
        # Structure: dsf_dict[permno] = dataframe of daily returns
        dsf_grouped = {p: group for p, group in crsp_dsf.groupby('permno')}
        
        def compute_car(row):
            f_date = row['filing_date']
            p_no = row['permno']
            if p_no in dsf_grouped:
                firm_df = dsf_grouped[p_no]
                # Filter limits
                bounds = firm_df[(firm_df['date'] >= f_date - pd.to_timedelta(1, unit='d')) & 
                                 (firm_df['date'] <= f_date + pd.to_timedelta(3, unit='d'))]
                if len(bounds) > 0:
                    return np.prod(1 + bounds['ret']) - 1
            return np.nan
            
        valid_events['CAR_T1_T3'] = valid_events.apply(compute_car, axis=1)
        df_aligned = pd.merge(df_aligned, valid_events[['gvkey', 'fyear', 'CAR_T1_T3']], on=['gvkey', 'fyear'], how='left')
    else:
        df_aligned['CAR_T1_T3'] = np.nan
        print("   No valid filing dates identified. Skipping CAR event study.")

    # ---------------------------------------------------------
    # PHASE 7: WINSORIZATION & SANITIZATION
    # ---------------------------------------------------------
    print("\n[PHASE 7] Imposing Strict Outlier Sterilization (Winsorizing & Validation)...")
    
    # 1. Target invalid firm health
    starting_rows = len(df_aligned)
    df_aligned = df_aligned[df_aligned['at'] > 0]
    df_aligned = df_aligned.dropna(subset=['ret']) # We must have a structural y-variable prediction
    dropped = starting_rows - len(df_aligned)
    print(f"   Dropped {dropped} structural anomalies (Assets < 0 or Return Missing)")
    
    # 2. Winsorize (Clamping extreme 1% tails)
    tail_clamped = ['ROA', 'Leverage', 'ret', 'Excess_Return', 'ME', 'CAR_T1_T3', 'SGA_intensity']
    for var in tail_clamped:
        if var in df_aligned.columns:
            mask = df_aligned[var].notna()
            if mask.sum() > 0:
                df_aligned.loc[mask, var] = winsorize(df_aligned.loc[mask, var], limits=[0.01, 0.01])
    print("   Data clamped securely at the 0.01 and 0.99 quantiles.")

    out_file = r"C:\Users\Admin\Downloads\New folder\final_analysis_panel.csv"
    df_aligned.to_csv(out_file, index=False)
    
    print("="*50)
    print(f" [SUCCESS] Master Structural File Generated: \n -> {out_file}")
    print(f" [METRICS] Final Matrix Shape: {df_aligned.shape}")
    print("="*50)

if __name__ == "__main__":
    main()
