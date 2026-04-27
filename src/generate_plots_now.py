import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

csv_filename = r"C:\Users\Admin\Downloads\New folder\comprehensive_risk_analysis.csv"
output_dir = r"C:\Users\Admin\.gemini\antigravity\scratch\risk_plots"
os.makedirs(output_dir, exist_ok=True)

if os.path.exists(csv_filename):
    final_df = pd.read_csv(csv_filename)
    final_df = final_df[final_df['word_count'] > 0]
    norm_cols = [c for c in final_df.columns if "_norm" in c]
    
    if not final_df.empty:
        txt_path = os.path.join(output_dir, "top_10_risk_companies.txt")
        with open(txt_path, "w") as f:
            for norm_col in norm_cols:
                f.write(f"\n--- TOP 10 FOR: {norm_col.upper()} ---\n")
                top_10 = final_df.sort_values(by=norm_col, ascending=False).head(10)
                f.write(top_10[['coname', 'ticker', norm_col]].to_string(index=False) + "\n")
        
        print(f"Top 10 list saved to {txt_path}")
        
        print("Generating plots...")
        for norm_col in norm_cols:
            plt.figure(figsize=(10, 4))
            yearly_trend = final_df.groupby('Year')[norm_col].mean()
            risk_name = norm_col.replace('_norm', '').replace('_', ' ').title()
            
            plt.plot(yearly_trend.index, yearly_trend.values, marker='o', color='teal')
            plt.title(f"Trend for Risk Exposure: {risk_name} Over the Years")
            plt.xlabel("Year")
            plt.ylabel("Avg Mentions per 10k words")
            # Force integer ticks for years
            plt.xticks(yearly_trend.index, [str(int(y)) for y in yearly_trend.index], rotation=45)
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            plt.savefig(os.path.join(output_dir, f"{norm_col}_trend.png"))
            plt.close()

        plt.figure(figsize=(15, 12))
        corr = final_df[norm_cols].corr()
        sns.heatmap(corr, annot=False, cmap='coolwarm', linewidths=0.1)
        plt.title("Correlation Matrix of Risk Exposure Keywords")
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, "correlation_matrix.png"))
        plt.close()

        print(f"All plots saved to {output_dir}")
else:
    print("CSV not found.")
