"""
Data generation module for payments reconciliation testing.

This module generates realistic transaction and settlement data with specific
gap patterns planted for testing reconciliation algorithms.
"""

import random
from datetime import datetime, timedelta
from decimal import Decimal

import faker
import pandas as pd
from faker import Faker

from models.schemas import (
    TransactionRecord,
    SettlementRecord,
    TransactionStatus,
    Currency,
)


def generate_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Generate transaction and settlement data with planted gaps.
    
    Returns:
        Tuple of (transactions_df, settlements_df) containing generated data
    """
    fake = Faker()
    Faker.seed(42)  # For reproducible data
    random.seed(42)
    
    transactions = []
    settlements = []
    
    # Generate 1000 transactions for January 2024
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 1, 31, 23, 59, 59)
    
    for i in range(1000):
        # Generate random datetime in January 2024
        created_at = start_date + timedelta(
            seconds=random.randint(0, int((end_date - start_date).total_seconds()))
        )
        
        # Determine status - 5 refunds, rest completed
        if i < 5:
            status = TransactionStatus.REFUNDED
        else:
            status = TransactionStatus.COMPLETED
        
        # Generate amount between 10.00 and 5000.00 with exactly 2 decimal places
        amount = Decimal(str(round(random.uniform(10, 5000), 2))).quantize(Decimal('0.01'))
        
        transaction = TransactionRecord(
            transaction_id=f"TXN-{i+1:04d}",
            amount=amount,
            currency=Currency.USD,
            status=status,
            created_at=created_at,
            customer_id=f"CUST-{random.randint(1000, 9999)}"
        )
        transactions.append(transaction)
        
        # Generate matching settlement (1-2 days later)
        settlement_delay = timedelta(days=random.randint(1, 2))
        settled_at = created_at + settlement_delay
        
        settlement = SettlementRecord(
            settlement_id=f"SET-{i+1:04d}",
            transaction_ref=transaction.transaction_id,
            amount=amount,
            settled_at=settled_at,
            batch_id=f"BATCH-{random.randint(1, 50):03d}"
        )
        settlements.append(settlement)
    
    # Plant GAP 1: TIMING - transaction index 100 (TXN-0101) settled in February
    gap1_tx_id = "TXN-0101"
    for settlement in settlements:
        if settlement.transaction_ref == gap1_tx_id:
            settlement = SettlementRecord(
                settlement_id=settlement.settlement_id,
                transaction_ref=settlement.transaction_ref,
                amount=settlement.amount,
                settled_at=datetime(2024, 2, 3, 10, 30),  # February 3, 2024
                batch_id=settlement.batch_id
            )
            settlements[100] = settlement
            break
    
    # Plant GAP 2: ROUNDING - transactions index 200-299 (TXN-0201 to TXN-0300)
    # Add 0.01 to each settlement amount (will be visible after quantization)
    for i in range(200, 300):
        tx_id = f"TXN-{i+1:04d}"
        for j, settlement in enumerate(settlements):
            if settlement.transaction_ref == tx_id:
                new_amount = settlement.amount + Decimal('0.01')
                new_settlement = SettlementRecord(
                    settlement_id=settlement.settlement_id,
                    transaction_ref=settlement.transaction_ref,
                    amount=new_amount.quantize(Decimal('0.01')),
                    settled_at=settlement.settled_at,
                    batch_id=settlement.batch_id
                )
                settlements[j] = new_settlement
                break
    
    # Plant GAP 3: DUPLICATE - transaction index 301 (TXN-0302)
    gap3_tx_id = "TXN-0302"
    original_settlement = None
    for settlement in settlements:
        if settlement.transaction_ref == gap3_tx_id:
            original_settlement = settlement
            break
    
    if original_settlement:
        duplicate_settlement = SettlementRecord(
            settlement_id="SET-DUPLICATE-001",
            transaction_ref=original_settlement.transaction_ref,
            amount=original_settlement.amount,
            settled_at=original_settlement.settled_at,
            batch_id=original_settlement.batch_id
        )
        settlements.append(duplicate_settlement)
    
    # Plant GAP 4: ORPHAN REFUND - non-existent transaction
    orphan_settlement = SettlementRecord(
        settlement_id="SET-ORPHAN-001",
        transaction_ref="GHOST-TXN-999",
        amount=Decimal("-150.00"),  # Negative refund amount
        settled_at=datetime(2024, 1, 25, 14, 30),
        batch_id="BATCH-999"
    )
    settlements.append(orphan_settlement)
    
    # Convert to DataFrames, preserving Decimal as strings
    transactions_data = []
    for tx in transactions:
        transactions_data.append({
            'transaction_id': tx.transaction_id,
            'amount': str(tx.amount),
            'currency': tx.currency.value,
            'status': tx.status.value,
            'created_at': tx.created_at.isoformat(),
            'customer_id': tx.customer_id
        })
    
    settlements_data = []
    for st in settlements:
        settlements_data.append({
            'settlement_id': st.settlement_id,
            'transaction_ref': st.transaction_ref,
            'amount': str(st.amount),
            'settled_at': st.settled_at.isoformat(),
            'batch_id': st.batch_id
        })
    
    transactions_df = pd.DataFrame(transactions_data)
    settlements_df = pd.DataFrame(settlements_data)
    
    # Save to CSV files
    transactions_df.to_csv('data/transactions.csv', index=False)
    settlements_df.to_csv('data/settlements.csv', index=False)
    
    # Print summary
    print(f"Generated {len(transactions)} transactions")
    print(f"Generated {len(settlements)} settlements")
    print("Planted gaps:")
    print(f"  GAP 1 (TIMING): {gap1_tx_id}")
    print(f"  GAP 2 (ROUNDING): TXN-0201 to TXN-0300 (100 transactions)")
    print(f"  GAP 3 (DUPLICATE): {gap3_tx_id}")
    print(f"  GAP 4 (ORPHAN REFUND): GHOST-TXN-999")
    
    return transactions_df, settlements_df


if __name__ == '__main__':
    generate_data()
