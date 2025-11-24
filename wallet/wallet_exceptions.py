class WalletError(Exception):
    """Base exception for wallet operations."""
    pass


class InsufficientFundsError(WalletError):
    """Raised when sender has insufficient balance."""
    pass


class SameUserTransferError(WalletError):
    """Raised when trying to transfer to the same user."""
    pass


class WalletNotFoundError(WalletError):
    """Raised when wallet doesn't exist."""
    pass
