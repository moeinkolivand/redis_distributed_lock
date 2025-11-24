from typing import TypedDict, Literal


class User(TypedDict):
    user_id: str
    full_name: str
    email: str
    created_at: str
    status: Literal["active", "inactive", "suspended"]


class Wallet(TypedDict):
    wallet_id: str
    user_id: str
    balance: float
    currency: Literal["USD", "EUR", "GBP"]
    created_at: str
    status: Literal["active", "frozen", "closed"]
