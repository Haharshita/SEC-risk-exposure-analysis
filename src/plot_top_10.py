import pandas as pd
import matplotlib.pyplot as plt
import os

# Try to find the CSV
csv_paths = [
    r"C:\Users\Admin\Downloads\New folder\comprehensive_risk_analysis.csv",
]

csv_used = None
for p in csv_paths:
    if os.path.exists(p):
        csv_used = p
        break

if not csv_used:
    print("CSV not found.")
    exit(1)

print(f"Reading from {csv_used}...")
df = pd.read_csv(csv_used, dtype={'cik': str})
df = df[df['word_count'] > 0]

# Calculate total risk count across all categories
risk_cols = [c for c in df.columns if c.endswith('_count')]
df['total_risk_count'] = df[risk_cols].sum(axis=1)

# Group by company
company_risk = df.groupby(['cik', 'coname', 'ticker'])['total_risk_count'].sum().reset_index()
top_10 = company_risk.sort_values(by='total_risk_count', ascending=False).head(10)

print("--- TOP 10 COMPANIES BY OVERALL RISK EXPOSURE ---")
print(top_10[['coname', 'ticker', 'total_risk_count']].to_string(index=False))

# Plot
plt.figure(figsize=(12, 6))
plt.barh(top_10['coname'], top_10['total_risk_count'], color='teal')
plt.xlabel('Total Risk Keyword Mentions (Aggregated across active years)')
plt.title('Top 10 Companies by Total Risk Mentions')
plt.gca().invert_yaxis()
plt.tight_layout()
out_png = r"C:\Users\Admin\.gemini\antigravity\scratch\top_10_companies_overall.png"
plt.savefig(out_png)
print(f"Plot saved to: {out_png}")
