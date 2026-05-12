import pandas as pd
import os

# Define the 5 macro categories and their corresponding normalized column names
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

csv_filename = r"C:\Users\Admin\Downloads\New folder\comprehensive_risk_analysis.csv"
output_filename = r"C:\Users\Admin\Downloads\New folder\categorized_risk_analysis.csv"

if os.path.exists(csv_filename):
    print(f"Loading {csv_filename}...")
    df = pd.read_csv(csv_filename)
    
    print("Calculating category scores...")
    for category, columns in CATEGORIES.items():
        # Check which columns actually exist in the dataframe to prevent KeyError
        existing_cols = [col for col in columns if col in df.columns]
        if existing_cols:
            # Sum the normalized scores of the individual keywords to get the macro category score
            df[f"{category}_category_score"] = df[existing_cols].sum(axis=1)
        else:
            print(f"Warning: No valid columns found for category {category}")
            df[f"{category}_category_score"] = 0
            
    print(f"Saving categorized data to {output_filename}...")
    df.to_csv(output_filename, index=False)
    print("Data aggregation complete! You can now run generate_plots_fixed.py")
else:
    print(f"Error: {csv_filename} not found. Please ensure your risk_exposure.py has generated it.")
