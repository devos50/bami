"""
Contains code related to FROST.
"""
import os

EMPTY_PK = b'\x00' * 64
MSG = b'\01' * 32 #os.urandom(32)
