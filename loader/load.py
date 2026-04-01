"""
Data loading module for payments reconciliation system.

This module handles loading CSV data into DuckDB with validation using Pydantic models
and proper decimal typing for financial calculations.
"""

import duckdb
from decimal import Decimal
from pathlib import Path

import pandas as pd

from models.schemas import TransactionRecord, SettlementRecord, Currency, TransactionStatus


def load_data(db_path: str = 'data/recon.duckdb') -> duckdb.DuckDBPyConnection:
    """
    Load and validate transaction and settlement data into DuckDB.
    
    Args:
        db_path: Path to the DuckDB database file
        
    Returns:
        Open DuckDB connection
        
    Raises:
        ValueError: If any row fails validation
    """
    # Ensure data directory exists
    Path('data').mkdir(exist_ok=True)
    
    # Read CSV files
    transactions_df = pd.read_csv('data/transactions.csv')
    settlements_df = pd.read_csv('data/settlements.csv')
    
    # Validate every transaction row
    for idx, row in transactions_df.iterrows():
        try:
            amount = Decimal(row['amount']).quantize(Decimal('0.01'))
            TransactionRecord(
                transaction_id=row['transaction_id'],
                amount=amount,
                currency=Currency(row['currency']),
                status=TransactionStatus(row['status']),
                created_at=pd.to_datetime(row['created_at']).to_pydatetime(),
                customer_id=row['customer_id']
            )
        except Exception as e:
            raise ValueError(f"Transaction validation failed at row {idx + 1}: {e}")
    
    # Validate every settlement row
    for idx, row in settlements_df.iterrows():
        try:
            amount = Decimal(row['amount']).quantize(Decimal('0.01'))
            SettlementRecord(
                settlement_id=row['settlement_id'],
                transaction_ref=row['transaction_ref'],
                amount=amount,
                settled_at=pd.to_datetime(row['settled_at']).to_pydatetime(),
                batch_id=row['batch_id']
            )
        except Exception as e:
            raise ValueError(f"Settlement validation failed at row {idx + 1}: {e}")
    
    # Create/recreate database
    conn = duckdb.connect(db_path)
    
    # Drop existing tables if they exist
    conn.execute("DROP TABLE IF EXISTS transactions")
    conn.execute("DROP TABLE IF EXISTS settlements")
    
    # Create transactions table with DECIMAL(18,2) for amount
    conn.execute("""
        CREATE TABLE transactions (
            transaction_id VARCHAR PRIMARY KEY,
            amount DECIMAL(18,2) NOT NULL,
            currency VARCHAR NOT NULL,
            status VARCHAR NOT NULL,
            created_at TIMESTAMP NOT NULL,
            customer_id VARCHAR NOT NULL
        )
    """)
    
    # Create settlements table with DECIMAL(18,2) for amount
    conn.execute("""
        CREATE TABLE settlements (
            settlement_id VARCHAR PRIMARY KEY,
            transaction_ref VARCHAR NOT NULL,
            amount DECIMAL(18,2) NOT NULL,
            settled_at TIMESTAMP NOT NULL,
            batch_id VARCHAR NOT NULL
        )
    """)
    
    # Insert data into transactions table
    conn.execute("""
        INSERT INTO transactions 
        SELECT transaction_id, amount, currency, status, created_at, customer_id
        FROM transactions_df
    """)
    
    # Insert data into settlements table
    conn.execute("""
        INSERT INTO settlements 
        SELECT settlement_id, transaction_ref, amount, settled_at, batch_id
        FROM settlements_df
    """)
    
    return conn


def get_summary(conn: duckdb.DuckDBPyConnection) -> dict:
    """
    Get summary statistics from the database.
    
    Args:
        conn: DuckDB connection
        
    Returns:
        Dictionary with summary statistics as Decimals
    """
    # Get transaction count and amount sum
    tx_summary = conn.execute("""
        SELECT 
            COUNT(*) as count,
            COALESCE(SUM(amount), 0) as total_amount
        FROM transactions
    """).fetchone()
    
    # Get settlement count and amount sum
    set_summary = conn.execute("""
        SELECT 
            COUNT(*) as count,
            COALESCE(SUM(amount), 0) as total_amount
        FROM settlements
    """).fetchone()
    
    tx_count, tx_total = tx_summary
    set_count, set_total = set_summary
    
    # Convert to Decimal for precision
    tx_total_decimal = Decimal(str(tx_total))
    set_total_decimal = Decimal(str(set_total))
    
    return {
        'transaction_count': tx_count,
        'settlement_count': set_count,
        'transaction_sum': tx_total_decimal,
        'settlement_sum': set_total_decimal,
        'net_difference': tx_total_decimal - set_total_decimal
    }
