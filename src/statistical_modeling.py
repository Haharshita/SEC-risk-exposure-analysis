import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import statsmodels.api as sm
from sklearn.ensemble import RandomForestRegressor

# Beautiful styling for our plots
sns.set_theme(style="whitegrid", context="talk", palette="crest")

def run_univariate_ols_with_controls(df, target, keyword_cols, controls, output_file):
    with open(output_file, 'a', encoding="utf-8") as f:
        f.write(f"\n{'='*60}\n")
        f.write(f"  TARGET: {target} ".center(60, '='))
        f.write(f"\n{'='*60}\n")
        f.write("Individual OLS Regressions (Macro Category + Controls)\n")
        f.write(f"Controls used: {', '.join(controls)}\n\n")
        
        # Prepare valid data intersection
        valid_df = df.dropna(subset=[target] + controls + keyword_cols)
        
        if valid_df.empty:
            f.write("ERROR: No valid overlapping data for target and controls.\n")
            return
            
        results_summary = []
        
        for kw in keyword_cols:
            if valid_df[kw].sum() == 0:
                continue # Skip dead keywords
                
            X = valid_df[[kw] + controls]
            X = sm.add_constant(X)
            y = valid_df[target]
            
            try:
                model = sm.OLS(y, X).fit()
                coef = model.params[kw]
                tval = model.tvalues[kw]
                pval = model.pvalues[kw]
                
                # Highlight significant findings aesthetically
                sig = "***" if pval < 0.01 else "**" if pval < 0.05 else "*" if pval < 0.1 else ""
                
                results_summary.append({
                    'Keyword': kw.replace('_category_score', '').replace('_', ' ').title(),
                    'Coefficient': coef,
                    't-stat': tval,
                    'p-value': pval,
                    'Significance': sig
                })
            except Exception as e:
                pass
                
        # Sort and display by significance 
        results_summary = sorted(results_summary, key=lambda x: x['p-value'])
        
        f.write(f"{'Macro Category'.ljust(35)} | {'Coef'.rjust(10)} | {'t-stat'.rjust(10)} | {'p-val'.rjust(10)}\n")
        f.write('-'*70 + '\n')
        
        for res in results_summary:
            f.write(f"{res['Keyword'][:34].ljust(35)} | {res['Coefficient']:10.4f} | {res['t-stat']:10.2f} | {res['p-value']:10.4f} {res['Significance']}\n")

def run_random_forest_importance(df, target, keyword_cols, controls, output_png_path):
    print(f"[*] Training Random Forest mapping for {target}...")
    valid_df = df.dropna(subset=[target] + keyword_cols)
    
    if valid_df.empty:
        print(f"[-] Skipped Random Forest for {target} due to missing overlap.")
        return
        
    X = valid_df[keyword_cols] # Purposely omitting controls to strictly rank the intrinsic keywords against themselves
    y = valid_df[target]
    
    # Train robust Random forest
    rf = RandomForestRegressor(n_estimators=150, max_depth=10, random_state=42, n_jobs=-1)
    rf.fit(X, y)
    
    importance = rf.feature_importances_
    
    # Map arrays back to readable names
    clean_names = [kw.replace('_category_score', '').replace('_', ' ').title() for kw in keyword_cols]
    
    feature_df = pd.DataFrame({
        'Macro Category': clean_names,
        'Importance': importance
    }).sort_values(by='Importance', ascending=False)
    
    # High-aesthetic Visualization
    plt.figure(figsize=(10, 6))
    
    # Define color mappings
    ax = sns.barplot(x='Importance', y='Macro Category', data=feature_df, palette='magma')
    ax.set_title(f"Macro Category Market Drivers (Random Forest)\nTarget: {target}", fontsize=18, pad=20, fontweight='bold')
    ax.set_xlabel('Relative Feature Importance (Non-linear Impact Metrics)', fontsize=14)
    ax.set_ylabel('')
    
    # Add borders and adjust gracefully
    sns.despine(left=True, bottom=True)
    plt.tight_layout()
    plt.savefig(output_png_path, dpi=300, bbox_inches='tight')
    plt.close()

def main():
    print("="*50)
    print(" SEC Risk Exposure: Phase 8 Statistical Analysis ")
    print("="*50)

    # 1. Load the sterilized pipeline output
    input_file = r"C:\Users\Admin\Downloads\New folder\final_analysis_panel.csv"
    if not os.path.exists(input_file):
        print(f"[-] ERROR: Panel data not found at {input_file}")
        print("    -> Please ensure you have fully run the market_data_pipeline.py first!")
        return
        
    print(f"[+] Loading completely structured panel dataset from {input_file}...")
    df = pd.read_csv(input_file)
    
    # 2. Vector Identification - Map the 40 raw keywords to 5 macro categories
    CATEGORIES = {
        "Core_Cloud_and_Data_Center": [
            "data_center_norm", "server_farm_norm", "colocation_norm", "hyperscaler_norm", 
            "edge_computing_norm", "cloud_infrastructure_norm", "data_warehousing_norm", 
            "cloud_computing_norm", "gpu_cluster_norm", "infrastructure_as_a_service_norm"
        ],
        "Network_and_Performance": [
            "low_latency_norm", "high_bandwidth_norm", "network_infrastructure_norm", 
            "streaming_infrastructure_norm", "content_delivery_network_norm", 
            "uptime_norm", "scalability_norm", "redundancy_norm"
        ],
        "Emerging_Tech_and_AI": [
            "artificial_intelligence_norm", "machine_learning_norm", "big_data_analytics_norm", 
            "internet_of_things_norm", "fintech_platform_norm", "high-frequency_trading_norm", 
            "digital_asset_norm", "omnichannel_strategy_norm"
        ],
        "Cyber_and_Resilience_Risk": [
            "cybersecurity_infrastructure_norm", "disaster_recovery_norm", 
            "data_sovereignty_norm", "power_usage_effectiveness_norm"
        ],
        "Strategic_Tech_Investment": [
            "digital_transformation_norm", "ict_investment_norm", 
            "capital_expenditure_in_technology_norm", "digital_infrastructure_norm", 
            "real_estate_technology_investment_norm", "tech-intensive_assets_norm", 
            "infrastructure_investment_norm", "technological_innovation_norm", 
            "it_capital_allocation_norm", "facility_modernization_norm"
        ]
    }

    # Generate the category scores natively on the fly
    keyword_cols = []
    for cat, cols in CATEGORIES.items():
        cat_col = f"{cat}_category_score"
        valid_cols = [c for c in cols if c in df.columns]
        if valid_cols:
            df[cat_col] = df[valid_cols].sum(axis=1)
            keyword_cols.append(cat_col)
            
    print(f"[+] Successfully aggregated data into {len(keyword_cols)} Macro Categories.")
    
    possible_controls = ['Size', 'Leverage', 'ROA', 'CapEx_ratio', 'RD_intensity', 'ME']
    controls = [c for c in possible_controls if c in df.columns]
    
    targets = []
    if 'Excess_Return' in df.columns: targets.append('Excess_Return')
    if 'CAR_T1_T3' in df.columns: targets.append('CAR_T1_T3')
    
    if not targets:
        print("[-] Critical Failure: No target return variables (Excess_Return, CAR_T1_T3) identified in the CSV.")
        return

    # 3. Output Allocation
    output_txt = r"C:\Users\Admin\Downloads\New folder\regression_results.txt"
    # Write Header
    with open(output_txt, 'w', encoding="utf-8") as f:
        f.write("SEC PIPELINE STATISTICAL MODELING ECONOMETRICS REPORT\n")
        f.write("="*60 + "\n")
        f.write(f"Observations Loaded: {len(df)}\n")
        f.write(f"Categories Tested: {len(keyword_cols)}\n")
        f.write("Significance Legend: *** (p<0.01), ** (p<0.05), * (p<0.1)\n")
        
    # 4. Engine Process
    for target in targets:
        run_univariate_ols_with_controls(df, target, keyword_cols, controls, output_txt)
        
        out_png = os.path.join(r"C:\Users\Admin\Downloads\New folder", f"macro_category_importance_{target.lower()}.png")
        run_random_forest_importance(df, target, keyword_cols, controls, out_png)
        
    print(f"\n[+] Statistical Pipeline Successfully Complete!")
    print(f"    -> Regressions Report: {output_txt}")
    print(f"    -> Plotted Visual Outputs deployed in C:\\Users\\Admin\\Downloads\\New folder\\")

if __name__ == "__main__":
    main()
