from loader.load import load_data
from engine.reconcile import run_reconciliation, get_gap_summary
from models.schemas import GapType
from decimal import Decimal

conn = load_data()

# Debug: Check the raw data
query = """
    SELECT 
        t.transaction_id,
        s.settlement_id,
        t.amount as transaction_amount,
        s.amount as settlement_amount,
        t.created_at,
        s.settled_at
    FROM transactions t
    JOIN settlements s ON t.transaction_id = s.transaction_ref
    WHERE t.transaction_id = 'TXN-0001'
"""
debug_row = conn.execute(query).fetchdf()
print("Debug row:")
print(debug_row)
print("Transaction amount type:", type(debug_row['transaction_amount'].iloc[0]))
print("Settlement amount type:", type(debug_row['settlement_amount'].iloc[0]))
print("Transaction amount:", debug_row['transaction_amount'].iloc[0])
print("Settlement amount:", debug_row['settlement_amount'].iloc[0])

print('Running reconciliation...')
results = run_reconciliation(conn)
