import logging
import os
import time
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


class CodeJob:
    code: Code
    logger: Logger

    def __init__(self, code: Code, logger: Logger) -> None:
        self.code = code
        self.logger = logger

    def __call__(self) -> None:
        result = self.code.run_with_std_patch()
        code_logs = f"\n----job::{self.code.name}----\n{result}\n-------------------------------------\n"
        self.logger.code_logger.info(code_logs)
        return {self.code.name: result}


def cronify(cron_config: Dict):
    key = " | ".join(map(lambda x: f"{x[0]}:'{x[1]}'", cron_config.items()))
    return f"< {key} >"


executors = {"default": pool.ThreadPoolExecutor(20)}
scheduler = BlockingScheduler(executors=executors)

state = State()
logger = Logger()

FETCH_HOUR = os.environ.get("FETCH_CRON_HOUR", "*")
FETCH_MINUTE = os.environ.get("FETCH_CRON_MINUTE", "*/15")


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
            buckets.add(cronify(job.cron))
            del_ids.add(str(job.id))

        for bucket in buckets:
            state.store[bucket] = CodeList(
                filter(
                    lambda code_obj: code_obj.name not in del_ids, state.store[bucket]
                )
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

    for job in jobs:
        code_obj = Code(str(job.id), job.code)
        state.store[cronify(job.cron)].append(code_obj)
        scheduler.add_job(CodeJob(code_obj, logger), "cron", **job.cron, id=str(job.id))

    logger.root_logger.info("scheduled %s jobs", str(num_jobs))

    if num_jobs > 0:
        logger.pip_logger.info("After fetch_store: Store: %s", str(state.store))

    jobs.update(
        set__sync=True,
    )  # set the processed jobs as in sync with in-memory store

    return state


def main():
    setup_logging()
    logger = Logger()
    logger.root_logger = logging.getLogger("root")
    logger.pip_logger = logging.getLogger("pip")
    logger.code_logger = logging.getLogger("code")

    connect(db="scheduler", host=get_URI("scheduler"))
    logger.root_logger.info("Connected to DB")

    state = State()
    state.store = Store()
    logger.root_logger.info("Created in-memory store")
    logger.root_logger.info("Current store:\n%s\n", state.store)

    state.deps = get_deps()
    logger.pip_logger.info("Indexed deps: %s", state.deps)

    fetch_store(state, logger, fetch_all=True)
    scheduler.start()
    time.sleep(5)


if __name__ == "__main__":
    main()
