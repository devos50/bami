class Content:

    def __init__(self, identifier: bytes, data: bytes) -> None:
        self.data: bytes = data
        self.identifier: bytes = identifier

    def get_key(self) -> int:
        """
        Return the key of this content item, an integer between 0 and 2^32.
        """
        return int.from_bytes(self.data, 'big') % (2 ** 32)
