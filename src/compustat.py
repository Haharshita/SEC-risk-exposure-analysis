import wrds
import pandas as pd
import numpy as np
from sqlalchemy import text

def wrds_connection_patch(self, query):
    return self.connection.execute(text(query))

wrds.sql.Connection.load_library_list = lambda self: \
    set(row[0] for row in wrds_connection_patch(self, "SELECT schemaname FROM (SELECT nspname AS schemaname FROM pg_namespace) s"))

username = 'rxg5597' 
db = wrds.Connection(wrds_username=username)

# PHASE 2 — EXTRACT & COMPUTE COMPUSTAT DATA
comp_query = """
SELECT gvkey, datadate, fyear, cik, 
        at, dltt, dlc, ib, capx, xrd, xsga, che, seq, txditc, pstk
FROM comp.funda
WHERE indfmt = 'INDL' 
  AND datafmt = 'STD' 
  AND consol = 'C' 
  AND popsrc = 'D'
  AND fyear >= 2016
  AND fyear < 2017
  AND at > 0
"""

comp_raw = db.raw_sql(comp_query)
comp_raw.to_csv('compustat_raw.csv', index=False)

comp_raw['Size'] = np.log(comp_raw['at'])
comp_raw['Leverage'] = (comp_raw['dltt'].fillna(0) + comp_raw['dlc'].fillna(0)) / comp_raw['at']
comp_raw['ROA'] = comp_raw['ib'] / comp_raw['at']
comp_raw['CapEx_ratio'] = comp_raw['capx'].fillna(0) / comp_raw['at']
comp_raw['RD_intensity'] = comp_raw['xrd'].fillna(0) / comp_raw['at']
comp_raw['SGA_intensity'] = comp_raw['xsga'].fillna(0) / comp_raw['at']
comp_raw['Cash_ratio'] = comp_raw['che'].fillna(0) / comp_raw['at']

comp_raw['BE'] = (comp_raw['seq'].fillna(0) + 
                  comp_raw['txditc'].fillna(0) - 
                  comp_raw['pstk'].fillna(0))

comp_raw.to_csv('compustat_firmyear.csv', index=False)

# PHASE 3 — LINK IDENTIFIERS (CCM)
ccm_query = """
SELECT gvkey, lpermno as permno, linktype, linkprim, linkdt, linkenddt
FROM crsp.ccmxpf_linktable
WHERE linktype IN ('LU', 'LC') 
  AND linkprim IN ('P', 'C')
"""

ccm_link = db.raw_sql(ccm_query)
ccm_link.to_csv('ccm_link.csv', index=False)

ccm_link['linkdt'] = pd.to_datetime(ccm_link['linkdt'])
ccm_link['linkenddt'] = pd.to_datetime(ccm_link['linkenddt'], errors='coerce').fillna(pd.Timestamp.today())
comp_raw['datadate'] = pd.to_datetime(comp_raw['datadate'])

final_df = pd.merge(comp_raw, ccm_link, on='gvkey', how='inner')

final_df = final_df[
    (final_df['datadate'] >= final_df['linkdt']) & 
    (final_df['datadate'] <= final_df['linkenddt'])
]

final_df.to_csv('final_linked_financials.csv', index=False)

db.close()

if __name__ == "__main__":
    print("SCRIPT EXECUTION FINISHED")