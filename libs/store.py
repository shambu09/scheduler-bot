"""
Store class to store all the code to execute according to schedule.
"""

from collections import UserDict, UserList

from libs.code import Code


class CodeList(UserList):
    def append(self, item: Code) -> None:
        if not isinstance(item, Code):
            raise TypeError("Can only append Code objects to the list")
        else:
            return super().append(item)


class Store(UserDict):
    def __setitem__(self, __k: str, __v: CodeList) -> None:
        if not isinstance(__v, CodeList):
            raise TypeError("Can only put CodeList objects in Store")
        if not isinstance(__k, str):
            raise TypeError("Can only use strings as keys in Store")
        else:
            return super().__setitem__(__k, __v)

    def __getitem__(self, __k: str):
        __v = None

        try:
            __v = super().__getitem__(__k)
        except KeyError:
            super().__setitem__(__k, CodeList())
            __v = super().__getitem__(__k)

        return __v
