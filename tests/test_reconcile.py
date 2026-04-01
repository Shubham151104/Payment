"""
Comprehensive test suite for the payments reconciliation system.

Tests verify gap detection, data integrity, and Pydantic model validation.
"""

import pytest
import pandas as pd
from decimal import Decimal
from hypothesis import given, strategies as st

from generator.generate import generate_data
from loader.load import load_data
from engine.reconcile import run_reconciliation
from models.schemas import (
    TransactionRecord,
    SettlementRecord,
    GapType,
    TransactionStatus,
    Currency,
)


@pytest.fixture(scope="session")
def db_conn():
    """
    Session-scoped fixture that runs the full pipeline once.
    
    Returns:
        Open DuckDB connection with loaded data
    """
    # Generate data
    generate_data()
    
    # Load data into DuckDB
    conn = load_data()
    
    yield conn
    
    # Cleanup
    conn.close()


@pytest.fixture(scope="session")
def recon_results(db_conn):
    """
    Session-scoped fixture that returns reconciliation results.
    
    Args:
        db_conn: DuckDB connection from db_conn fixture
        
    Returns:
        DataFrame with reconciliation results
    """
    return run_reconciliation(db_conn)


def test_timing_gap_detected(recon_results):
    """Assert at least 1 row with GapType.TIMING_GAP and that TXN-0101 has different months (planted gap)."""
    timing_gaps = recon_results[recon_results['gap_type'] == GapType.TIMING_GAP.value]
    
    assert len(timing_gaps) >= 1, f"Expected at least 1 TIMING_GAP, got {len(timing_gaps)}"
    
    # Verify the planted gap TXN-0101 has different months
    tx_0101 = timing_gaps[timing_gaps['transaction_id'] == 'TXN-0101']
    assert len(tx_0101) == 1, "Expected TXN-0101 to be classified as TIMING_GAP"
    
    row = tx_0101.iloc[0]
    assert row['created_at'].month != row['settled_at'].month, \
        f"Expected different months for TXN-0101: created_at={row['created_at']}, settled_at={row['settled_at']}"


def test_rounding_gap_detected(recon_results):
    """Assert at least 1 row with GapType.AMOUNT_MISMATCH and mismatch_reason mentions rounding or amount difference."""
    amount_mismatches = recon_results[recon_results['gap_type'] == GapType.AMOUNT_MISMATCH.value]
    
    assert len(amount_mismatches) >= 1, f"Expected at least 1 AMOUNT_MISMATCH, got {len(amount_mismatches)}"
    
    for _, row in amount_mismatches.iterrows():
        reason = str(row.get('mismatch_reason', '')).lower()
        assert 'amount' in reason or 'difference' in reason or 'rounding' in reason, \
            f"Expected mismatch_reason to mention amount/difference/rounding, got: {reason}"


def test_duplicate_detected(recon_results):
    """Assert at least 1 row with GapType.DUPLICATE and duplicated transaction_ref appears more than once in results."""
    duplicates = recon_results[recon_results['gap_type'] == GapType.DUPLICATE.value]
    
    assert len(duplicates) >= 1, f"Expected at least 1 DUPLICATE, got {len(duplicates)}"
    
    for _, row in duplicates.iterrows():
        tx_ref = row['transaction_id']
        ref_count = len(recon_results[recon_results['transaction_id'] == tx_ref])
        assert ref_count > 1, f"Expected transaction_ref {tx_ref} to appear more than once, got {ref_count}"


def test_orphaned_refund_detected(recon_results):
    """Assert exactly 1 row with GapType.ORPHANED_REFUND, null transaction_id, and negative settlement amount."""
    orphan_refs = recon_results[recon_results['gap_type'] == GapType.ORPHANED_REFUND.value]
    
    assert len(orphan_refs) == 1, f"Expected exactly 1 ORPHANED_REFUND, got {len(orphan_refs)}"
    
    row = orphan_refs.iloc[0]
    assert pd.isna(row['transaction_id']), \
        f"Expected null transaction_id for orphaned refund, got {row['transaction_id']}"
    assert row['settlement_amount'] < 0, \
        f"Expected negative settlement amount, got {row['settlement_amount']}"


def test_no_unclassified_rows(recon_results):
    """Assert zero rows have a null gap_type."""
    unclassified = recon_results[recon_results['gap_type'].isna()]
    assert len(unclassified) == 0, f"Found {len(unclassified)} unclassified rows with null gap_type"


def test_matched_rows_are_majority(recon_results):
    """Assert more than 80% of rows are MATCHED."""
    total_rows = len(recon_results)
    matched = recon_results[recon_results['gap_type'] == GapType.MATCHED.value]
    matched_count = len(matched)
    
    matched_percentage = (matched_count / total_rows) * 100
    assert matched_percentage > 80, \
        f"Expected >80% MATCHED rows, got {matched_percentage:.1f}% ({matched_count}/{total_rows})"


@given(st.decimals(min_value=Decimal('0.01'), max_value=Decimal('9999.99'), places=2))
def test_all_amounts_are_decimal_precision(amount):
    """
    Property test using Hypothesis: create a minimal TransactionRecord and assert 
    that the amount survives a round-trip through Pydantic validation with no precision loss.
    """
    from datetime import datetime
    
    original_amount = Decimal(str(amount))
    
    tx = TransactionRecord(
        transaction_id='TEST-TXN-001',
        amount=original_amount,
        currency=Currency.USD,
        status=TransactionStatus.COMPLETED,
        created_at=datetime(2024, 1, 15, 10, 30),
        customer_id='CUST-TEST-001'
    )
    
    stored_amount = tx.amount
    
    assert original_amount == stored_amount, \
        f"Amount precision lost: original={original_amount}, stored={stored_amount}"
