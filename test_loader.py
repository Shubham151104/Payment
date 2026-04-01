from loader.load import load_data, get_summary
from decimal import Decimal

# Step 1: Load data into DuckDB
print('Loading data into DuckDB...')
conn = load_data()
print('✓ DuckDB loaded without errors')

# Step 2: Verify table row counts match CSVs
txn_count = conn.execute('SELECT COUNT(*) FROM transactions').fetchone()[0]
set_count = conn.execute('SELECT COUNT(*) FROM settlements').fetchone()[0]
assert txn_count == 1000, f'✗ Expected 1000 transactions in DB, got {txn_count}'
print(f'✓ Transactions in DB: {txn_count}')
assert set_count >= 1001, f'✗ Expected 1001+ settlements in DB, got {set_count}'
print(f'✓ Settlements in DB: {set_count}')

# Step 3: Verify DECIMAL type enforced (not DOUBLE or VARCHAR)
schema = conn.execute('DESCRIBE transactions').fetchall()
amount_col = [col for col in schema if col[0] == 'amount']
assert amount_col, '✗ No amount column found in transactions table'
assert 'DECIMAL' in amount_col[0][1].upper(), f'✗ Amount column is not DECIMAL type: {amount_col[0][1]}'
print(f'✓ Amount column type enforced: {amount_col[0][1]}')

# Step 4: Verify summary function works and net difference is non-zero
summary = get_summary(conn)
assert summary['net_difference'] != Decimal('0'), '✗ Net difference is zero — rounding gap not reflected'
print(f'✓ Net difference is non-zero: {summary["net_difference"]}')
print(f'  Transaction sum: {summary["transaction_sum"]}')
print(f'  Settlement sum:  {summary["settlement_sum"]}')

conn.close()
print('--- Checkpoint 3 PASSED ---')
