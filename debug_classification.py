from loader.load import load_data
from engine.reconcile import run_reconciliation
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
print("TXN-0001 data:")
print(df)

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

# Manual classification check
row = df.iloc[0]
transaction_id = row['transaction_id']
settlement_id = row['settlement_id']
transaction_amount = Decimal(str(row['transaction_amount']))
settlement_amount = Decimal(str(row['settlement_amount']))

print(f"Transaction ID: {transaction_id}")
print(f"Settlement ID: {settlement_id}")
print(f"Transaction Amount: {transaction_amount}")
print(f"Settlement Amount: {settlement_amount}")
print(f"Amounts equal: {transaction_amount == settlement_amount}")
print(f"Amount difference: {abs(transaction_amount - settlement_amount)}")
print(f"Greater than 0.00: {abs(transaction_amount - settlement_amount) > Decimal('0.00')}")

conn.close()
