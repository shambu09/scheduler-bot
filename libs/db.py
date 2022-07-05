"""
Schema:
    hour
    deps
    code
    sync
"""

from typing import List

from mongoengine import BooleanField, Document, ListField, StringField


class Job(Document):
    hour: str = StringField(required=True)
    deps: List[str] = ListField(required=True)
    code: str = StringField(required=True)
    name: str = StringField(default="")
    sync: bool = BooleanField(default=False)
    to_be_deleted: bool = BooleanField(default=False)