from loader.load import load_data
from decimal import Decimal

conn = load_data()

# Check transactions 201-300 for rounding gap
query = """
    SELECT 
        t.transaction_id,
        CAST(t.amount AS VARCHAR) as transaction_amount,
        CAST(s.amount AS VARCHAR) as settlement_amount,
        CAST(t.amount AS DECIMAL) - CAST(s.amount AS DECIMAL) as difference
    FROM transactions t
    JOIN settlements s ON t.transaction_id = s.transaction_ref
    WHERE t.transaction_id BETWEEN 'TXN-0201' AND 'TXN-0300'
    ORDER BY t.transaction_id
"""
rounding_check = conn.execute(query).fetchdf()
print("Rounding gap check for transactions 201-300:")
print(rounding_check.head(10))
print(f"Total rows: {len(rounding_check)}")

# Check if any have differences
differences = rounding_check[rounding_check['difference'] != 0]
print(f"Rows with differences: {len(differences)}")
if len(differences) > 0:
    print("Sample differences:")
    print(differences.head())

conn.close()
