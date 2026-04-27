import os
import re
import csv
import time
import shutil
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from sec_edgar_downloader import Downloader

csv_path = r"C:\Users\Admin\Downloads\New folder\comprehensive_risk_analysis.csv"

# Load the exact same keywords to maintain consistency
risk_keywords = [
    "Data center", "Server farm", "Colocation", "Hyperscaler", "Edge computing",
    "Cloud infrastructure", "Data warehousing", "Cloud computing", "GPU cluster",
    "Infrastructure as a Service", "Low latency", "High bandwidth", "Redundancy",
    "Scalability", "Power usage effectiveness", "Uptime", "Data sovereignty",
    "Disaster recovery", "Cybersecurity infrastructure", "Network infrastructure",
    "Artificial Intelligence", "Machine learning", "Big data analytics",
    "Digital transformation", "Internet of Things", "Omnichannel strategy",
    "High-frequency trading", "Streaming infrastructure", "Content Delivery Network",
    "FinTech platform", "ICT investment", "Digital asset", 
    "Capital expenditure in technology", "Digital infrastructure",
    "Real estate technology investment", "Tech-intensive assets",
    "Infrastructure investment", "Technological innovation",
    "IT capital allocation", "Facility modernization"
]
patterns = {f"{k.replace(' ', '_').lower()}_count": rf"\b{re.escape(k)}s?\b" for k in risk_keywords}
output_columns = ["cik", "ticker", "coname", "Year", "word_count"] + list(patterns.keys()) + [k.replace('_count', '_norm') for k in patterns.keys()]

def preprocess_text(soup):
    for table in soup.find_all("table"):
        table.decompose()
    text = soup.get_text(separator=' ').lower()
    contractions = {
        "don't": "do not", "can't": "cannot", "it's": "it is", "isn't": "is not",
        "aren't": "are not", "won't": "will not", "shouldn't": "should not",
        "i'm": "i am", "you're": "you are", "we're": "we are", "they're": "they are"
    }
    for contraction, expanded in contractions.items():
        text = text.replace(contraction, expanded)

    text = re.sub(r'https?://\S+|www\.\S+', '', text)
    text = re.sub(r'[^a-z\s]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()

    # We must keep a copy of the basic clean text for regex phrase matching
    # otherwise stop-word removal and length filtering completely breaks multi-word keywords.
    basic_text = text
    
    tokens = word_tokenize(text)
    stop_words = set(stopwords.words('english'))
    lemmatizer = WordNetLemmatizer()
    tokens = [lemmatizer.lemmatize(w) for w in tokens if w not in stop_words and len(w) > 2]
    return basic_text, " ".join(tokens)

print("Reading CIKs...")
unique_ciks = []
try:
    with open(r"C:\Users\Admin\Downloads\New folder\ciks.txt", "r") as f:
        unique_ciks = sorted(list(set([line.strip() for line in f.readlines() if line.strip()])))
except Exception as e:
    print(f"Error reading ciks.txt: {e}")
    unique_ciks = ["0000320193"]

# Figure out exactly what needs to be downloaded
processed_pairs = set()
if os.path.exists(csv_path):
    df_existing = pd.read_csv(csv_path, usecols=['cik', 'Year'], dtype={'cik': str})
    df_existing = df_existing.dropna(subset=['cik', 'Year'])
    for _, row in df_existing.iterrows():
        processed_pairs.add((str(row['cik']).lstrip('0'), int(row['Year'])))

dl = Downloader("Emergeflow-Technologies-Recovery", "harshita@emergeflow.com")

print("Starting deep recovery process with Auto-Resume enabled...")
for year in range(2008, 2027):
    print(f"\\n--- Recovering Year {year} ---")
    
    for cik in unique_ciks:
        raw_cik = str(cik).lstrip('0')
        if (raw_cik, year) in processed_pairs:
            continue
            
        base_dir = "sec-edgar-filings"
        if os.path.exists(base_dir):
            try: shutil.rmtree(base_dir)
            except Exception: pass
            
        # Robust Download Loop
        max_retries = 3
        base_sleep = 5
        success = False
        for attempt in range(max_retries):
            try:
                dl.get("10-K", cik, after=f"{year}-01-01", before=f"{year+1}-01-01", download_details=False)
                time.sleep(0.2)  # Throttle normally
                success = True
                break
            except Exception as e:
                print(f"  Attempt {attempt+1}/{max_retries} failed for {cik}: {e}. Backing off {base_sleep}s...")
                time.sleep(base_sleep)
                base_sleep *= 2
        
        results = []
        if success and os.path.exists(base_dir):
            for root, dirs, files in os.walk(base_dir):
                for file in files:
                    if file.endswith((".html", ".txt")):
                        file_path = os.path.join(root, file)
                        
                        try:
                            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                                file_content = f.read()
                                if not file_content.strip(): continue
                                
                                basic_text, processed_text = preprocess_text(BeautifulSoup(file_content, 'lxml'))
                                word_count = len(processed_text.split())
                                if word_count == 0: continue
            
                                row_data = {
                                    "cik": raw_cik, "ticker": "Unknown", "coname": "Unknown", # Kept minimal for recovery wrapper
                                    "Year": year, "word_count": word_count
                                }
                                
                                for label, regex in patterns.items():
                                    count = len(re.findall(regex, basic_text, re.IGNORECASE))
                                    row_data[label] = count
                                    norm_name = label.replace("_count", "_norm")
                                    row_data[norm_name] = (count / word_count) * 10000
                                
                                results.append(row_data)
                        except Exception as e:
                            print(f"  Parse error on {cik}: {e}")
                            
            try: shutil.rmtree(base_dir)
            except Exception: pass

        if not results:
            # Re-add standard empty placeholder logic so the main script stays synced if it reads it again
            results.append({
                "cik": raw_cik, "ticker": "N/A", "coname": "Unknown",
                "Year": year, "word_count": -1 
            })
            
        df_batch = pd.DataFrame(results)
        for col in output_columns:
            if col not in df_batch.columns:
                df_batch[col] = 0
        df_batch = df_batch[output_columns]
        
        df_batch.to_csv(csv_path, mode='a', index=False, header=not os.path.exists(csv_path))
        processed_pairs.add((raw_cik, year))
        print(f"  Recovered CIK {cik} for {year} {'(Found Data)' if results[0]['word_count'] > 0 else '(Did Not File)'}")
