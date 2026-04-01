from generator.generate import generate_data
from loader.load import load_data
from engine.reconcile import run_reconciliation
from models.schemas import GapType

generate_data()
conn = load_data()
results = run_reconciliation(conn)

print('=== WHAT IS STORED IN gap_type COLUMN ===')
print(repr(results['gap_type'].iloc[0]))
print(type(results['gap_type'].iloc[0]))

print()
print('=== WHAT GapType.TIMING_GAP.value IS ===')
print(repr(GapType.TIMING_GAP.value))
print(type(GapType.TIMING_GAP.value))

print()
print('=== ALL UNIQUE VALUES IN gap_type COLUMN ===')
for v in results['gap_type'].unique():
    print(repr(v))

conn.close()
