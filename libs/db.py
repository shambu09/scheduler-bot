"""
Schema:
    day
    hour
    minute
    deps
    code
    name
    sync
    to_be_deleted
    timezone
"""

from typing import Dict, List

from mongoengine import (BooleanField, DictField, Document, ListField,
                         StringField)


class Job(Document):
    # CRON
    cron: Dict = DictField(
        required=True
    )  # year, month, day, week, day_of_week, hour, min, second

    # CODE
    deps: List[str] = ListField(required=True)
    code: str = StringField(required=True)
    name: str = StringField(default="")

    # METADATA
    sync: bool = BooleanField(default=False)
    to_be_deleted: bool = BooleanField(default=False)
    timezone: str = StringField(default=None)
