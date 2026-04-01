from loader.load import load_data
from engine.reconcile import run_reconciliation, get_gap_summary
from models.schemas import GapType

conn = load_data()
print('Running reconciliation...')
results = run_reconciliation(conn)

# Step 1: No unclassified rows
assert results['gap_type'].notna().all(), '✗ Some rows have no gap_type'
print(f'✓ All {len(results)} rows classified')

# Step 2: All 4 gap types detected
gap_summary = get_gap_summary(results)
print('Gap summary:', gap_summary)

gap_types_found = results['gap_type'].unique()
assert GapType.TIMING_GAP.value in gap_types_found, '✗ TIMING_GAP not detected'
print('✓ TIMING_GAP detected')

assert GapType.AMOUNT_MISMATCH.value in gap_types_found, '✗ AMOUNT_MISMATCH not detected'
print('✓ AMOUNT_MISMATCH detected')

assert GapType.DUPLICATE.value in gap_types_found, '✗ DUPLICATE not detected'
print('✓ DUPLICATE detected')

assert GapType.ORPHANED_REFUND.value in gap_types_found, '✗ ORPHANED_REFUND not detected'
print('✓ ORPHANED_REFUND detected')

# Step 3: Matched rows are the majority
matched_count = len(results[results['gap_type'] == GapType.MATCHED.value])
assert matched_count > 800, f'✗ Too few matched rows: {matched_count}'
print(f'✓ Matched rows: {matched_count}')

conn.close()
print('--- Checkpoint 4 PASSED ---')
