import pandas as pd
import requests

csv_path = r"C:\Users\Admin\Downloads\New folder\comprehensive_risk_analysis.csv"

print("1. Downloading historical SEC CIK lookup data...")
headers = {"User-Agent": "harshita@emergeflow.com"}
r = requests.get('https://www.sec.gov/Archives/edgar/cik-lookup-data.txt', headers=headers)
r.raise_for_status()

# Format of file is: Company Name:CIK:
# E.g. !J INC:0001438823:
print("2. Parsing master lookup index...")
lines = r.text.split('\n')
historical_cik_map = {}
for line in lines:
    parts = line.split(":")
    if len(parts) >= 2 and parts[-2].isdigit():
        cik = parts[-2]
        # In case the company name contains colons intrinsically
        coname = ":".join(parts[:-2]).strip()
        historical_cik_map[cik] = coname.title()

print(f"   Loaded {len(historical_cik_map):,} historical CIK definitions.")

print("3. Loading analytical dataset...")
df = pd.read_csv(csv_path, dtype={'cik': str})

# Zero pad all CIKs to 10 digits to correctly match the SEC master format
df['padded_cik'] = df['cik'].astype(str).str.zfill(10)

unknown_mask = df['coname'].str.lower() == 'unknown'
unknown_count = unknown_mask.sum()
print(f"4. Located {unknown_count:,} rows with 'Unknown' company names in the dataset.")

if unknown_count > 0:
    print("5. Retroactively mapping authentic historical names...")
    # Inject mapped values
    resolved_names = df.loc[unknown_mask, 'padded_cik'].map(historical_cik_map)
    df.loc[unknown_mask, 'coname'] = resolved_names.fillna('Unknown')
    
    # Calculate success
    resolved_count = (df.loc[unknown_mask, 'coname'].str.lower() != 'unknown').sum()
    print(f"Success! Discovered and repaired the historical name for {resolved_count:,} records!")
    
    # Save back safely
    print("6. Overwriting comprehensive_risk_analysis.csv natively...")
    df.drop(columns=['padded_cik'], inplace=True)
    df.to_csv(csv_path, index=False)
    print("Complete!")

else:
    print("No unknowns found to resolve!")
