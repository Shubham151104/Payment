from loader.load import load_data
from engine.reconcile import run_reconciliation, get_gap_summary
from models.schemas import GapType

conn = load_data()
results = run_reconciliation(conn)

print("Unique gap types found:")
print(results['gap_type'].unique())

print("\nGap summary:")
gap_summary = get_gap_summary(results)
for gap_type, stats in gap_summary.items():
    print(f"{gap_type}: {stats['count']} rows")

conn.close()
