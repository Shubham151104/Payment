"""
Streamlit dashboard for payments reconciliation system.

Provides interactive visualization of reconciliation results with filtering
and detailed gap analysis.
"""

import streamlit as st
import pandas as pd
from decimal import Decimal
import sys

# Handle missing dependencies gracefully
try:
    from generator.generate import generate_data
    from loader.load import load_data, get_summary
    from engine.reconcile import run_reconciliation
    from models.schemas import GapType
    DEPENDENCIES_AVAILABLE = True
except ImportError as e:
    st.error(f"Missing dependency: {e}")
    st.error("This app requires the full payments reconciliation system to be installed.")
    st.stop()
    DEPENDENCIES_AVAILABLE = False


@st.cache_resource
def load_reconciliation_data():
    """Load and process reconciliation data once per session."""
    generate_data()
    conn = load_data()
    results = run_reconciliation(conn)
    summary = get_summary(conn)
    conn.close()
    return results, summary


def format_decimal(amount):
    """Format Decimal amount for display with 2 decimal places."""
    if isinstance(amount, Decimal):
        return f"${amount:.2f}"
    return f"${Decimal(str(amount)):.2f}"


def main():
    if not DEPENDENCIES_AVAILABLE:
        st.error("Cannot run dashboard - missing dependencies")
        return
    
    st.set_page_config(page_title="Payments Reconciliation", layout="wide")
    
    with st.spinner("Loading reconciliation data..."):
        results, summary = load_reconciliation_data()
    
    st.title("Payments Reconciliation Dashboard — January 2024")
    
    # Calculate metrics
    total_transactions = summary['transaction_count']
    total_settlements = summary['settlement_count']
    net_difference = summary['net_difference']
    
    # Count gaps (non-MATCHED rows)
    gaps = results[results['gap_type'] != GapType.MATCHED.value]
    total_gaps = len(gaps)
    
    # Display metric cards
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Transactions", total_transactions)
    
    with col2:
        st.metric("Total Settlements", total_settlements)
    
    with col3:
        delta_color = "inverse" if net_difference != 0 else "normal"
        st.metric("Net Amount Difference", format_decimal(net_difference), delta=net_difference, delta_color=delta_color)
    
    with col4:
        st.metric("Total Gaps Found", total_gaps)
    
    st.divider()
    
    # Gap distribution chart
    st.subheader("Gap Distribution")
    gap_counts = results['gap_type'].value_counts()
    st.bar_chart(gap_counts)
    
    st.divider()
    
    # Filterable table
    st.subheader("Reconciliation Results")
    
    gap_types = results['gap_type'].unique()
    selected_gaps = st.multiselect(
        "Filter by Gap Type",
        options=gap_types,
        default=gap_types,
        key="gap_filter"
    )
    
    if selected_gaps:
        filtered_results = results[results['gap_type'].isin(selected_gaps)]
    else:
        filtered_results = pd.DataFrame()
    
    if not filtered_results.empty:
        # Format amounts for display
        display_df = filtered_results.copy()
        display_df['transaction_amount'] = display_df['transaction_amount'].apply(lambda x: format_decimal(x))
        display_df['settlement_amount'] = display_df['settlement_amount'].apply(lambda x: format_decimal(x))
        
        # Display selected columns
        display_columns = [
            'transaction_id', 'settlement_id', 'transaction_amount', 
            'settlement_amount', 'gap_type', 'mismatch_reason'
        ]
        st.dataframe(display_df[display_columns], use_container_width=True)
    else:
        st.info("No results to display. Select gap types to filter.")
    
    st.divider()
    
    # Gap details expanders
    st.subheader("Gap Details")
    
    with st.expander("🕐 TIMING_GAP"):
        st.write("""
        **What it means:** A settlement was processed in a different calendar month than the original transaction.
        
        **Why it happens in production:**
        - Batch processing delays causing end-of-month transactions to settle in the next month
        - Weekend or holiday processing delays
        - System maintenance windows
        - Cross-border settlement timing differences
        
        **Business impact:** Cash flow timing discrepancies, month-end reporting adjustments needed.
        """)
    
    with st.expander("💰 AMOUNT_MISMATCH"):
        st.write("""
        **What it means:** The settlement amount differs from the original transaction amount.
        
        **Why it happens in production:**
        - Currency conversion rate fluctuations between transaction and settlement
        - Processing fees or charges applied during settlement
        - Partial settlements or split payments
        - System rounding errors in high-volume processing
        
        **Business impact:** Revenue reconciliation issues, customer billing disputes, audit findings.
        """)
    
    with st.expander("📋 DUPLICATE"):
        st.write("""
        **What it means:** Multiple settlement records exist for the same transaction.
        
        **Why it happens in production:**
        - System retries causing duplicate settlement attempts
        - Manual reprocessing of failed transactions
        - Integration issues between payment processors
        - Data replication lag creating apparent duplicates
        
        **Business impact:** Overpayment risks, customer refunds needed, accounting errors.
        """)
    
    with st.expander("👻 ORPHANED_REFUND"):
        st.write("""
        **What it means:** A settlement (typically negative/refund) exists without a corresponding original transaction.
        
        **Why it happens in production:**
        - Manual refunds processed outside the main system
        - Legacy system data migration issues
        - Chargeback processing from payment networks
        - System data corruption or incomplete transaction records
        
        **Business impact:** Unexplained cash outflows, audit trail gaps, compliance issues.
        """)
    
    st.divider()
    
    # Sum check at bottom
    st.subheader("Sum Check")
    sum_check_text = (
        f"Sum check — Transactions: {format_decimal(summary['transaction_sum'])} | "
        f"Settlements: {format_decimal(summary['settlement_sum'])} | "
        f"Difference: {format_decimal(summary['net_difference'])}"
    )
    st.markdown(f"**{sum_check_text}**")


if __name__ == "__main__":
    main()
