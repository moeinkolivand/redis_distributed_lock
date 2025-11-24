from pydantic import BaseModel, Field, field_validator
from decimal import Decimal
from typing import Literal

class TransferRequested(BaseModel):
    transfer_id: str = Field(..., description="Unique transfer ID")
    from_user: str = Field(..., min_length=1, pattern=r"^[A-Za-z0-9_-]+$")
    to_user: str = Field(..., min_length=1, pattern=r"^[A-Za-z0-9_-]+$")
    amount: Decimal = Field(..., gt=0, decimal_places=2, max_digits=18)
    currency: Literal["USD", "EUR", "GBP"] = Field(default="USD")
    idempotency_key: str = Field(default_factory=lambda: "")

    @field_validator("idempotency_key", mode="before")
    @classmethod
    def default_idempotency_key(cls, v, info):
        return v or info.data.get("transfer_id", "")

    @field_validator("amount", mode="before")
    @classmethod
    def parse_amount(cls, v):
        if isinstance(v, (int, float, str)):
            return Decimal(str(v))
        return v


class TransferCompleted(BaseModel):
    transfer_id: str
    status: Literal["COMPLETED", "FAILED"] = "COMPLETED"
    processed_at: float
    from_user: str
    to_user: str
    amount: Decimal
    currency: str = "USD"