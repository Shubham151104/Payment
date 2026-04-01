from loader.load import load_data
from engine.reconcile import run_reconciliation, get_gap_summary
from models.schemas import GapType
from decimal import Decimal

conn = load_data()

# Debug: Check the raw data
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
debug_row = conn.execute(query).fetchdf()
print("Debug row:")
print(debug_row)

# Check if it's in duplicate set
duplicate_refs = conn.execute("""
    SELECT transaction_ref 
    FROM settlements 
    WHERE transaction_ref != 'GHOST-TXN-999'
    GROUP BY transaction_ref 
    HAVING COUNT(*) > 1
""").fetchdf()
duplicate_set = set(duplicate_refs['transaction_ref'].tolist())
print("Is TXN-0001 in duplicate set?", 'TXN-0001' in duplicate_set)
