"""
Store class to store all the code to execute according to schedule.
"""

from collections import UserDict, UserList
from typing import Iterable

from libs.code import Code


class CodeList(UserList):
    def append(self, item: Code) -> None:
        if not isinstance(item, Code):
            raise TypeError("Can only append Code objects to the list")
        else:
            return super().append(item)


class Store(UserDict):
    def __init__(self, keys: Iterable[str]) -> None:
        super().__init__()

        for key in keys:
            super().__setitem__(key, CodeList())

    def __setitem__(self, __k: str, __v: CodeList) -> None:
        if not isinstance(__v, CodeList):
            raise TypeError("Can only put CodeList objects in Store")
        elif super().get(__k, None) == None:
            raise TypeError("Store object cannot create new key-value pairs")
        else:
            return super().__setitem__(__k, __v)
