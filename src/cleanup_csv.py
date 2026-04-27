import pandas as pd
import sys
sys.stdout.reconfigure(encoding='utf-8')

csv_path = r"C:\Users\Admin\Downloads\New folder\comprehensive_risk_analysis.csv"

# Load the file
df = pd.read_csv(csv_path, dtype={'cik': str})
original_count = len(df)

# Filter out placeholder rows (word_count <= 0)
df_clean = df[df['word_count'] > 0]
new_count = len(df_clean)

# Save the cleaned file
df_clean.to_csv(csv_path, index=False)

print(f"✅ Cleanup Complete!")
print(f"Original rows: {original_count}")
print(f"Valid rows remaining: {new_count}")
print(f"Deleted {original_count - new_count} bad placeholders!")
