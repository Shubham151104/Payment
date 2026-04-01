import traceback

try:
    from generator.generate import generate_data
    from loader.load import load_data
    from engine.reconcile import run_reconciliation

    generate_data()
    conn = load_data()
    
    print('=== ATTEMPTING RECONCILIATION ===')
    results = run_reconciliation(conn)
    print(f'Rows returned: {len(results)}')
    print(results['gap_type'].value_counts())
    conn.close()

except Exception as e:
    print(f'=== ERROR ===')
    print(f'Type: {type(e).__name__}')
    print(f'Message: {e}')
    print()
    print('=== FULL TRACEBACK ===')
    traceback.print_exc()
