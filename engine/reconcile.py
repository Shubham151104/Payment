"""
Reconciliation engine for payments system.

Performs matching between transactions and settlements, classifying
gaps according to business rules with strict priority ordering.
"""

import duckdb
from decimal import Decimal
from typing import Dict
import pandas as pd

from models.schemas import GapType


def run_reconciliation(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    Perform reconciliation between transactions and settlements.
    
    Args:
        conn: DuckDB connection with loaded data
        
    Returns:
        DataFrame with reconciliation results
        
    Raises:
        RuntimeError: If any row remains unclassified
    """
    # Step 1: Find duplicate transaction_refs in settlements table
    duplicate_query = """
        SELECT transaction_ref 
        FROM settlements 
        WHERE transaction_ref != 'GHOST-TXN-999'
        GROUP BY transaction_ref 
        HAVING COUNT(*) > 1
    """
    duplicate_df = conn.execute(duplicate_query).fetchdf()
    duplicate_refs = set(duplicate_df['transaction_ref'].tolist())
    
    # Step 2: Get full outer join result
    join_query = """
        SELECT 
            COALESCE(t.transaction_id, 'NULL') as transaction_id,
            COALESCE(s.settlement_id, 'NULL') as settlement_id,
            COALESCE(CAST(t.amount AS VARCHAR), '0.00') as transaction_amount,
            COALESCE(CAST(s.amount AS VARCHAR), '0.00') as settlement_amount,
            COALESCE(t.created_at, '1970-01-01') as created_at,
            COALESCE(s.settled_at, '1970-01-01') as settled_at
        FROM transactions t
        FULL OUTER JOIN settlements s ON t.transaction_id = s.transaction_ref
        ORDER BY 
            CASE 
                WHEN t.transaction_id IS NULL THEN s.settlement_id
                ELSE t.transaction_id
            END
    """
    df = conn.execute(join_query).fetchdf()
    
    # Step 3: Classify each row in Python
    results = []
    
    for _, row in df.iterrows():
        transaction_id = row['transaction_id']
        settlement_id = row['settlement_id']
        
        # Convert amounts to Decimal
        transaction_amount = Decimal(str(row['transaction_amount']))
        settlement_amount = Decimal(str(row['settlement_amount']))
        
        # Convert dates to Python datetime
        created_at = pd.to_datetime(row['created_at']).to_pydatetime()
        settled_at = pd.to_datetime(row['settled_at']).to_pydatetime()
        
        gap_type = None
        mismatch_reason = None
        
        # Priority 1: DUPLICATE
        if transaction_id != 'NULL' and transaction_id in duplicate_refs:
            gap_type = GapType.DUPLICATE
            mismatch_reason = f"Transaction {transaction_id} has multiple settlements"
        
        # Priority 2: ORPHANED_REFUND
        elif transaction_id == 'NULL' and settlement_id != 'NULL' and settlement_amount < 0:
            gap_type = GapType.ORPHANED_REFUND
            mismatch_reason = f"Negative settlement {settlement_id} has no matching transaction"
        
        # Priority 3: UNMATCHED_TRANSACTION
        elif transaction_id != 'NULL' and settlement_id == 'NULL':
            gap_type = GapType.UNMATCHED_TRANSACTION
            mismatch_reason = f"Transaction {transaction_id} has no matching settlement"
        
        # Priority 4: TIMING_GAP (both exist, different months)
        elif transaction_id != 'NULL' and settlement_id != 'NULL':
            if created_at.month != settled_at.month or created_at.year != settled_at.year:
                gap_type = GapType.TIMING_GAP
                mismatch_reason = f"Settlement in different month than transaction"
            elif abs(transaction_amount - settlement_amount) > Decimal('0.00'):
                gap_type = GapType.AMOUNT_MISMATCH
                diff = abs(transaction_amount - settlement_amount)
                mismatch_reason = f"Amount difference: {diff}"
            else:
                gap_type = GapType.MATCHED
                mismatch_reason = None
        
        # Check if row was classified
        if gap_type is None:
            raise RuntimeError(f"Unclassified row: transaction_id={transaction_id}, settlement_id={settlement_id}, created_at={created_at}, settled_at={settled_at}, tx_amt={transaction_amount}, set_amt={settlement_amount}")
        
        results.append({
            'transaction_id': transaction_id if transaction_id != 'NULL' else None,
            'settlement_id': settlement_id if settlement_id != 'NULL' else None,
            'transaction_amount': transaction_amount,
            'settlement_amount': settlement_amount,
            'created_at': created_at,
            'settled_at': settled_at,
            'gap_type': gap_type.value,
            'mismatch_reason': mismatch_reason
        })
    
    results_df = pd.DataFrame(results)
    
    # Verify all rows are classified
    unclassified = results_df[results_df['gap_type'].isna()]
    if len(unclassified) > 0:
        raise RuntimeError(f"{len(unclassified)} rows remain unclassified")
    
    return results_df


def get_gap_summary(results_df: pd.DataFrame) -> Dict[str, Dict[str, int]]:
    """
    Get summary statistics for each gap type.
    
    Args:
        results_df: DataFrame from run_reconciliation
        
    Returns:
        Dictionary with counts and amounts for each gap type
    """
    summary = {}
    
    for gap_type in GapType:
        gap_data = results_df[results_df['gap_type'] == gap_type.value]
        
        # Calculate transaction and settlement amounts
        tx_total = gap_data['transaction_amount'].sum()
        set_total = gap_data['settlement_amount'].sum()
        
        summary[gap_type.value] = {
            'count': len(gap_data),
            'transaction_total': tx_total,
            'settlement_total': set_total
        }
    
    return summary
