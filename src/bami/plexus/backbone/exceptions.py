class DatabaseDesynchronizedException(Exception):
    pass


class InvalidTransactionFormatException(Exception):
    pass


class InvalidBlockException(Exception):
    pass


class InvalidSubCommunityIdException(Exception):
    pass


class UnknownChainException(Exception):
    pass
