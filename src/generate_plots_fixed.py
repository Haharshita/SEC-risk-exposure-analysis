import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

csv_filename = r"C:\Users\Admin\Downloads\New folder\categorized_risk_analysis.csv"
output_dir = r"C:\Users\Admin\Downloads\New folder\Data_Visualizations"
os.makedirs(output_dir, exist_ok=True)

if os.path.exists(csv_filename):
    print(f"Loading {csv_filename}...")
    final_df = pd.read_csv(csv_filename)
    if 'word_count' in final_df.columns:
        final_df = final_df[final_df['word_count'] > 0]
    
    # Identify the newly created category score columns
    category_cols = [c for c in final_df.columns if "_category_score" in c]
    
    if not final_df.empty and category_cols:
        excel_path = os.path.join(output_dir, "top_10_risk_companies_by_category.xlsx")
        print(f"Writing top 10 companies to {excel_path}...")
        all_top_10 = []
        for cat_col in category_cols:
            top_10 = final_df.sort_values(by=cat_col, ascending=False).head(10).copy()
            top_10['Macro Category'] = cat_col.replace('_category_score', '').replace('_', ' ')
            top_10['Rank'] = range(1, 11)
            
            cols_to_keep = ['Macro Category', 'Rank']
            if 'coname' in top_10.columns and 'ticker' in top_10.columns:
                cols_to_keep.extend(['coname', 'ticker', 'cik'])
            elif 'cik' in top_10.columns:
                cols_to_keep.append('cik')
            
            cols_to_keep.append(cat_col)
            # Rename the score column for the Excel output
            top_10 = top_10[cols_to_keep].rename(columns={cat_col: 'Category Score (Mentions per 10k words)'})
            all_top_10.append(top_10)
            
        top_10_df = pd.concat(all_top_10, ignore_index=True)
        top_10_df.to_excel(excel_path, index=False)
        print(f"Top 10 list saved to {excel_path}")
        
        print("Generating trend plots...")
        for cat_col in category_cols:
            plt.figure(figsize=(10, 4))
            if 'Year' in final_df.columns:
                yearly_trend = final_df.groupby('Year')[cat_col].mean()
                cat_name = cat_col.replace('_category_score', '').replace('_', ' ')
                
                plt.plot(yearly_trend.index, yearly_trend.values, marker='o', color='teal')
                plt.title(f"Trend for Macro Risk Category: {cat_name} (2000-2025)")
                plt.xlabel("Year")
                plt.ylabel("Avg Category Score")
                plt.xticks(yearly_trend.index, [str(int(y)) for y in yearly_trend.index], rotation=45)
                plt.grid(True, alpha=0.3)
                plt.tight_layout()
                plt.savefig(os.path.join(output_dir, f"{cat_col}_trend.png"))
            plt.close()

        if len(category_cols) > 1:
            print("Generating correlation matrix...")
            plt.figure(figsize=(10, 8))
            corr = final_df[category_cols].corr()
            
            # Clean up labels for the heatmap
            clean_labels = [c.replace('_category_score', '').replace('_', ' ') for c in category_cols]
            corr.columns = clean_labels
            corr.index = clean_labels
            
            sns.heatmap(corr, annot=True, cmap='coolwarm', linewidths=0.1, fmt=".2f")
            plt.title("Correlation Matrix of Macro Risk Categories")
            plt.tight_layout()
            plt.savefig(os.path.join(output_dir, "category_correlation_matrix.png"))
            plt.close()

        print(f"All plots saved to {output_dir}")
    else:
        print("DataFrame is empty or no category score columns found. Make sure to run aggregate_categories.py first.")
else:
    print(f"CSV not found at {csv_filename}. Please run aggregate_categories.py first.")
