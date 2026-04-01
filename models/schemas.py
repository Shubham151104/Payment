"""
Data schemas for the payments reconciliation system.

This module defines the core data structures used throughout the reconciliation
process, including transaction records, settlement records, and reconciliation
results. All models are immutable and use strict type validation.
"""

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


class TransactionStatus(str, Enum):
    """Enumeration of possible transaction statuses in the payment system."""
    PENDING = "PENDING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    REFUNDED = "REFUNDED"


class Currency(str, Enum):
    """Enumeration of supported currencies for transactions and settlements."""
    USD = "USD"
    EUR = "EUR"
    GBP = "GBP"
    INR = "INR"


class TransactionRecord(BaseModel):
    """
    Immutable representation of a payment transaction.
    
    This model captures all essential information about a single transaction
    including its financial details, status, and timing information.
    """
    model_config = ConfigDict(frozen=True, strict=True)
    
    transaction_id: str = Field(..., description="Unique identifier for the transaction")
    amount: Decimal = Field(..., description="Transaction amount in specified currency")
    currency: Currency = Field(..., description="Currency of the transaction")
    status: TransactionStatus = Field(..., description="Current status of the transaction")
    created_at: datetime = Field(..., description="Timestamp when the transaction was created")
    customer_id: str = Field(..., description="Identifier of the customer who initiated the transaction")
    
    @field_validator('amount')
    @classmethod
    def validate_amount(cls, v: Decimal) -> Decimal:
        """Validate that amount has exactly 2 decimal places and is positive."""
        if not isinstance(v, Decimal):
            raise ValueError("Amount must be a Decimal, not float")
        if v <= 0:
            raise ValueError("Amount must be positive")
        if v.as_tuple().exponent != -2:
            raise ValueError("Amount must have exactly 2 decimal places")
        return v


class SettlementRecord(BaseModel):
    """
    Immutable representation of a payment settlement.
    
    This model captures information about how a transaction was settled,
    including the settlement amount, timing, and batch information.
    """
    model_config = ConfigDict(frozen=True, strict=True)
    
    settlement_id: str = Field(..., description="Unique identifier for the settlement")
    transaction_ref: str = Field(..., description="Reference to the original transaction ID")
    amount: Decimal = Field(..., description="Settlement amount in specified currency")
    settled_at: datetime = Field(..., description="Timestamp when the settlement was processed")
    batch_id: str = Field(..., description="Identifier of the settlement batch")
    
    @field_validator('amount')
    @classmethod
    def validate_amount(cls, v: Decimal) -> Decimal:
        """Validate that amount has exactly 2 decimal places (can be negative for refunds)."""
        if not isinstance(v, Decimal):
            raise ValueError("Amount must be a Decimal, not float")
        if v.as_tuple().exponent != -2:
            raise ValueError("Amount must have exactly 2 decimal places")
        return v


class GapType(str, Enum):
    """Enumeration of possible gap types identified during reconciliation."""
    MATCHED = "MATCHED"
    TIMING_GAP = "TIMING_GAP"
    AMOUNT_MISMATCH = "AMOUNT_MISMATCH"
    DUPLICATE = "DUPLICATE"
    ORPHANED_REFUND = "ORPHANED_REFUND"
    UNMATCHED_TRANSACTION = "UNMATCHED_TRANSACTION"


class ReconResult(BaseModel):
    """
    Immutable result of a reconciliation operation for a single record.
    
    This model represents the classification result after attempting to match
    a transaction with its corresponding settlement. It contains the original
    data (when available) and the classification result.
    """
    model_config = ConfigDict(frozen=True, strict=True)
    
    transaction: Optional[TransactionRecord] = Field(
        default=None, 
        description="Transaction record when available"
    )
    settlement: Optional[SettlementRecord] = Field(
        default=None, 
        description="Settlement record when available"
    )
    gap_type: GapType = Field(..., description="Classification of the reconciliation gap")
    mismatch_reason: Optional[str] = Field(
        default=None, 
        description="Detailed explanation of why records didn't match"
    )
