from typing import Dict, List

from mongoengine import *

from libs.db import Job
from utils import get_URI

# Sample
# jobs = [
#     {
#         "cron": {"hour":"11"},
#         "deps": ["flask"],
#         "code": f"import flask\nprint('flask')",
#     },
#     {
#         "cron": {"hour":"11"},
#         "deps": ["requests"],
#         "code": f"import requests\nprint('requests')",
#     },
#     {
#         "cron": {"hour":"12"},
#         "deps": ["Scrapy==2.6.1"],
#         "code": f"import flask\nprint('Scrapy==2.6.1')",
#     },
#     {
#         "cron": {"hour":"13"},
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
    jobs = [
        {
            "cron": {"hour": "*", "minute": "*", "second": "0/5"},
            "deps": ["Flask==2.1.2"],
            "code": """import datetime\nimport time\nprint(datetime.datetime.now(), flush=True)\ntime.sleep(10)\nprint(datetime.datetime.now(), flush=True)""",
        },
    ]

    setup()
    add_jobs(jobs)
