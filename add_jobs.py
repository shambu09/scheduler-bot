from typing import Dict, List

from mongoengine import *

from libs.db import Job
from utils import get_URI

jobs = [
    {
        "hour": "",
        "deps": [],
        "code": "",
        "name": "",
    },
]

# Sample
# jobs = [
#     {
#         "hour": "11",
#         "deps": ["flask"],
#         "code": f"import flask\nprint('flask')",
#     },
#     {
#         "hour": "11",
#         "deps": ["requests"],
#         "code": f"import requests\nprint('requests')",
#     },
#     {
#         "hour": "12",
#         "deps": ["Scrapy==2.6.1"],
#         "code": f"import flask\nprint('Scrapy==2.6.1')",
#     },
#     {
#         "hour": "17",
#         "deps": ["requests"],
#         "code": f"import requests\nprint('requests')",
#     },
# ]


def setup():
    connect(db="scheduler", host=get_URI("scheduler"))


def add_jobs(jobs: List[Dict]):
    rows = [Job(**job) for job in jobs]
    Job.objects.insert(rows)
    return rows


if __name__ == "__main__":
    setup()
    add_jobs(jobs)
