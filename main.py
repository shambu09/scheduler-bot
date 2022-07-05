import datetime
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Set

from apscheduler.executors import pool
from apscheduler.schedulers.background import BlockingScheduler
from mongoengine import *

from libs.code import Code
from libs.db import Job
from libs.singleton import Singleton
from libs.store import CodeList, Store
from utils import get_deps, get_URI, install_deps, setup_logging


class Logger(Singleton):
    root_logger: logging.Logger
    pip_logger: logging.Logger
    code_logger: logging.Logger


class State(Singleton):
    store: Store
    deps: Set[str]


executors = {"default": pool.ThreadPoolExecutor(5)}
scheduler = BlockingScheduler(executors=executors)

state = State()
logger = Logger()

FETCH_HOUR = os.environ.get("EXEC_CRON_HOUR", "*")
FETCH_MINUTE = os.environ.get("EXEC_CRON_MINUTE", "45")

EXEC_HOUR = os.environ.get("EXEC_CRON_HOUR", "*")
EXEC_MINUTE = os.environ.get("EXEC_CRON_MINUTE", "*/30")


def add_value():
    Job(hour="11", deps=["flask"], code=r"print('hello')", sync=False).save()
    Job(hour="11", deps=["requests"], code=r"print('hello')", sync=False).save()


@scheduler.scheduled_job(
    trigger="cron",
    hour=FETCH_HOUR,
    minute=FETCH_MINUTE,
    args=[state, logger],
)
def fetch_store(state: State, logger: Logger, fetch_all: bool = False) -> State:
    to_be_deleted_jobs = Job.objects(to_be_deleted=True)

    if len(to_be_deleted_jobs) > 0:
        logger.root_logger.info(f"To be deleted {len(to_be_deleted_jobs)} jobs")
        buckets = set()
        del_ids = set()

        for job in to_be_deleted_jobs:
            buckets.add(job.hour)
            del_ids.add(str(job.id))

        for hour in buckets:
            state.store[hour] = CodeList(
                filter(lambda code_obj: code_obj.name not in del_ids, state.store[hour])
            )

        for job in to_be_deleted_jobs:
            job.delete()

        logger.root_logger.info(
            "Deleted %s jobs:\n%s\n",
            str(len(del_ids)),
            str(del_ids),
        )

    needed_deps = set()
    if fetch_all:
        jobs = Job.objects()
    else:
        jobs = Job.objects(sync=False)

    num_jobs = 0
    for job in jobs:
        state.store[job.hour].append(Code(str(job.id), job.code))
        needed_deps.update(job.deps)
        num_jobs += 1

    logger.root_logger.info("fetched %s jobs", str(num_jobs))
    needed_deps = needed_deps - state.deps

    logger.root_logger.info(
        "Need to install %s deps",
        str(len(needed_deps)),
    )  # install deps which are not installed already and are required by the jobs

    if len(needed_deps) > 0:
        logger.root_logger.info(
            "Deps : %s",
            str(needed_deps),
        )

        pip_install_log = install_deps(needed_deps)
        logger.pip_logger.info("Pip install log:\n%s", pip_install_log)
        time.sleep(2)

        state.deps = get_deps()  # update deps
        logger.pip_logger.info(
            "After fetch_store: Deps available : %s", str(state.deps)
        )

    if num_jobs > 0:
        logger.pip_logger.info("After fetch_store: Store: %s", str(state.store))

    jobs.update(
        set__sync=True,
    )  # set the processed jobs as in sync with in-memory store

    return state


def run_jobs_of_nth_hour(n: str, state: State) -> Dict[str, str]:
    out = {}
    code_objs: CodeList = state.store[n]

    with ThreadPoolExecutor() as executor:
        out = {
            code_obj.name: executor.submit(code_obj.run_with_std_patch)
            for code_obj in code_objs
        }

    for job_id in out:
        out[job_id] = out[job_id].result()

    return out


@scheduler.scheduled_job(
    trigger="cron",
    hour=EXEC_HOUR,
    minute=EXEC_MINUTE,
    args=[state, logger],
)
def execute_jobs(state: State, logger: Logger) -> Dict[str, str]:
    hour = str(datetime.datetime.now().hour)
    out = run_jobs_of_nth_hour(hour, state)
    num_jobs = len(out)

    code_logs = {
        f"----job::{k}----\n{v}\n-------------------------------------\n"
        for k, v in out.items()
    }
    code_logs = "\n".join(code_logs)

    logger.code_logger.info(
        "Ran the jobs[%s] that were scheduled in the #%s hour bucket, outputs:\n\n%s",
        str(num_jobs),
        hour,
        code_logs,
    )

    return out


def main():
    setup_logging()
    logger = Logger()
    logger.root_logger = logging.getLogger("root")
    logger.pip_logger = logging.getLogger("pip")
    logger.code_logger = logging.getLogger("code")

    connect(db="scheduler", host=get_URI("scheduler"))
    logger.root_logger.info("Connected to DB")

    state = State()
    state.store = Store([str(i) for i in range(0, 24)])
    logger.root_logger.info("Created in-memory store")
    logger.root_logger.info("Current store:\n%s\n", state.store)

    state.deps = get_deps()
    logger.pip_logger.info("Indexed deps: %s", state.deps)

    fetch_store(state, logger, fetch_all=True)
    scheduler.start()
    time.sleep(5)


if __name__ == "__main__":
    main()
