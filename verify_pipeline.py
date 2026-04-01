print('=== FULL PIPELINE VERIFICATION ===')

import os
print(f'Working directory: {os.getcwd()}')

from generator.generate import generate_data
from loader.load import load_data, get_summary
from engine.reconcile import run_reconciliation, get_gap_summary
from models.schemas import GapType
from decimal import Decimal

txn_df, set_df = generate_data()
conn = load_data()
results = run_reconciliation(conn)
summary = get_summary(conn)
gaps = get_gap_summary(results)

print(f'Total rows in results: {len(results)}')
print(f'Gap types found: {results["gap_type"].unique().tolist()}')
print(f'Net difference: {summary["net_difference"]}')

# Fixed assertions — compare directly against unique values list, not str()
gap_types_found = results['gap_type'].unique().tolist()

assert len(results) > 0, 'No results returned'
assert summary['net_difference'] != Decimal('0'), 'Net difference is zero'
assert 'TIMING_GAP' in gap_types_found, f'TIMING_GAP missing. Found: {gap_types_found}'
assert 'AMOUNT_MISMATCH' in gap_types_found, f'AMOUNT_MISMATCH missing. Found: {gap_types_found}'
assert 'DUPLICATE' in gap_types_found, f'DUPLICATE missing. Found: {gap_types_found}'
assert 'ORPHANED_REFUND' in gap_types_found, f'ORPHANED_REFUND missing. Found: {gap_types_found}'
assert results['gap_type'].notna().all(), 'Some rows unclassified'

conn.close()

print()
print('✓ Data generation: OK')
print('✓ DuckDB loading: OK')
print('✓ Reconciliation engine: OK')
print('✓ All 4 gaps detected: OK')
print('✓ No unclassified rows: OK')
print(f'✓ Net difference: {summary["net_difference"]}')
print()
print('=== ALL SYSTEMS GO ===')
