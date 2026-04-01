from generator.generate import generate_data
from loader.load import load_data, get_summary
import duckdb

generate_data()
conn = load_data()

print('=== RAW TRANSACTIONS (first 3 rows) ===')
print(conn.execute('SELECT * FROM transactions LIMIT 3').df())

print()
print('=== RAW SETTLEMENTS (first 3 rows) ===')
print(conn.execute('SELECT * FROM settlements LIMIT 3').df())

print()
print('=== FULL OUTER JOIN (first 5 rows) ===')
join_result = conn.execute('''
    SELECT 
        t.transaction_id,
        s.settlement_id,
        t.amount as txn_amount,
        s.amount as set_amount,
        t.created_at,
        s.settled_at
    FROM transactions t
    FULL OUTER JOIN settlements s 
        ON t.transaction_id = s.transaction_ref
    LIMIT 5
''').df()
print(join_result)

print()
print('=== JOIN ROW COUNT ===')
count = conn.execute('''
    SELECT COUNT(*) FROM transactions t
    FULL OUTER JOIN settlements s 
        ON t.transaction_id = s.transaction_ref
''').fetchone()[0]
print(f'Total joined rows: {count}')

print()
print('=== TXN-0101 SPECIFICALLY ===')
row = conn.execute('''
    SELECT 
        t.transaction_id,
        s.settlement_id,
        t.created_at,
        s.settled_at
    FROM transactions t
    FULL OUTER JOIN settlements s 
        ON t.transaction_id = s.transaction_ref
    WHERE t.transaction_id = 'TXN-0101'
       OR s.transaction_ref = 'TXN-0101'
''').df()
print(row)

conn.close()