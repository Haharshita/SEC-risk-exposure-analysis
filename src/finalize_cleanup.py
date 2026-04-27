import pandas as pd

csv_path = r"C:\Users\Admin\Downloads\New folder\comprehensive_risk_analysis.csv"
print("Loading dataset...")
# keep_default_na=False prevents pandas from seeing the string 'N/A' as NaN!
df = pd.read_csv(csv_path, dtype={'cik': str})

print(f"Original Row Count: {len(df)}")

# 1. Fix the blanked out N/A tickers
blanks = df['ticker'].isnull().sum()
if blanks > 0:
    df['ticker'] = df['ticker'].fillna('N/A')
    print(f"Fixed {blanks} blank tickers back to 'N/A'.")

# 2. Drop the 6 sneaked placeholders
bad_rows = (df['word_count'] <= 0).sum()
if bad_rows > 0:
    df = df[df['word_count'] > 0]
    print(f"Purged {bad_rows} rogue placeholders.")

# 3. Drop exactly duplicate runs
dupes = df.duplicated(subset=['cik', 'Year']).sum()
if dupes > 0:
    # keep='last' typically keeps the most recent download attempt if there were retries
    df = df.drop_duplicates(subset=['cik', 'Year'], keep='last')
    print(f"Cleaned up {dupes} duplicate pipeline runs.")

print(f"Final Pristine Row Count: {len(df)}")
df.to_csv(csv_path, index=False)
print("File fully sanitized and saved!")
