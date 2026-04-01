from loader.load import load_data
import pandas as pd
from decimal import Decimal

conn = load_data()

# Debug TXN-0001 specifically
query = """
    SELECT 
        COALESCE(t.transaction_id, 'NULL') as transaction_id,
        COALESCE(s.settlement_id, 'NULL') as settlement_id,
        COALESCE(CAST(t.amount AS VARCHAR), '0.00') as transaction_amount,
        COALESCE(CAST(s.amount AS VARCHAR), '0.00') as settlement_amount,
        COALESCE(t.created_at, '1970-01-01') as created_at,
        COALESCE(s.settled_at, '1970-01-01') as settled_at
    FROM transactions t
    FULL OUTER JOIN settlements s ON t.transaction_id = s.transaction_ref
    WHERE t.transaction_id = 'TXN-0001'
"""
df = conn.execute(query).fetchdf()
row = df.iloc[0]

# Convert dates to Python datetime
created_at = pd.to_datetime(row['created_at']).to_pydatetime()
settled_at = pd.to_datetime(row['settled_at']).to_pydatetime()

print(f"Created at: {created_at} (month: {created_at.month})")
print(f"Settled at: {settled_at} (month: {settled_at.month})")
print(f"Same month: {created_at.month == settled_at.month}")
print(f"Same year: {created_at.year == settled_at.year}")

# Check duplicate refs
duplicate_query = """
    SELECT transaction_ref 
    FROM settlements 
    WHERE transaction_ref != 'GHOST-TXN-999'
    GROUP BY transaction_ref 
    HAVING COUNT(*) > 1
"""
duplicate_df = conn.execute(duplicate_query).fetchdf()
duplicate_refs = set(duplicate_df['transaction_ref'].tolist())
print(f"Is TXN-0001 in duplicates: {'TXN-0001' in duplicate_refs}")

conn.close()
