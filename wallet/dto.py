from pydantic import BaseModel, Field, field_validator, model_validator
from decimal import Decimal
from typing import Literal

class TransferRequested(BaseModel):
    transfer_id: str = Field(..., description="Unique transfer ID")
    from_user: str = Field(..., min_length=1, pattern=r"^[A-Za-z0-9_-]+$")
    to_user: str = Field(..., min_length=1, pattern=r"^[A-Za-z0-9_-]+$")
    amount: Decimal = Field(..., gt=0, decimal_places=2, max_digits=18)
    currency: Literal["USD", "EUR", "GBP"] = Field(default="USD")
    idempotency_key: str | None = None  # ← important: None means "missing"

    @field_validator("amount", mode="before")
    @classmethod
    def parse_amount(cls, v):
        if isinstance(v, (int, float, str)):
            return Decimal(str(v))
        return v

    @model_validator(mode="after")
    def set_default_idempotency_key(self):
        if not self.idempotency_key:  # None, "", etc.
            self.idempotency_key = self.transfer_id
        return self

class TransferCompleted(BaseModel):
    transfer_id: str
    status: Literal["COMPLETED", "FAILED"] = "COMPLETED"
    processed_at: float
    from_user: str
    to_user: str
    amount: Decimal
    currency: str = "USD"