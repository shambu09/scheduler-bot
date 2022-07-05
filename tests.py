import datetime
import logging

from mongoengine import *

from add_jobs import add_jobs
from libs.code import Code
from libs.db import Job
from libs.store import Store
from main import Logger, State, execute_jobs, fetch_store, run_jobs_of_nth_hour
from utils import get_deps, get_URI, setup_logging

jobs = [
    {
        "hour": "11",
        "deps": ["flask"],
        "code": f"import flask\nprint('flask')",
    },
    {
        "hour": "11",
        "deps": ["requests"],
        "code": f"import requests\nprint('requests')",
    },
    {
        "hour": "12",
        "deps": ["Scrapy==2.6.1"],
        "code": f"import flask\nprint('Scrapy==2.6.1')",
    },
    {
        "hour": "13",
        "deps": ["requests"],
        "code": f"import requests\nprint('requests')",
    },
]

jobs_out = [out["deps"][0] + "\n" for out in jobs]


class TestApp:
    @classmethod
    def setup_class(cls):
        """setup any state specific to the execution of the given class."""
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
        logger.root_logger.info(state.store)

        state.deps = get_deps()
        logger.pip_logger.info("Indexed deps: %s", state.deps)

    def test_add_jobs(self):
        rows = add_jobs(jobs)
        _rows = Job.objects(sync=False)

        for job in rows:
            assert job in _rows
            job.delete()

    def test_fetch_store(self):
        state = State()
        logger = Logger()
        hour = jobs[2]["hour"]
        dep = jobs[2]["deps"][0]
        code = jobs[2]["code"]

        job = Job(**jobs[2]).save()

        logger.root_logger.info(f"Before fetch_store: Job uploaded <{jobs[2].items()}>")

        logger.pip_logger.info(
            "Before fetch_store: Deps installed: %s", str(state.deps)
        )

        logger.pip_logger.info("Before fetch_store: Store: %s", str(state.store))

        logger.pip_logger.info(
            f"Before fetch_store: Scrapy installed = {dep in state.deps}"
        )

        logger.pip_logger.info(
            f"Before fetch_store: code present in #{hour} bucket = {any([code == code_obj.src for code_obj in state.store[hour]])}"
        )

        fetch_store(state, logger)

        logger.pip_logger.info("After fetch_store: Deps installed: %s", str(state.deps))

        logger.pip_logger.info("After fetch_store: Store: %s", str(state.store))

        logger.pip_logger.info(
            f"After fetch_store: {dep} installed = {dep in state.deps}"
        )

        logger.pip_logger.info(
            f"After fetch_store: code present in #{hour} bucket = {any([code == code_obj.src for code_obj in state.store[hour]])}"
        )

        job.delete()
        assert dep in state.deps
        assert any([code == code_obj.src for code_obj in state.store[hour]])

    def test_run_jobs_of_nth_hour(self):
        state = State()
        logger = Logger()

        rows = []
        for job in jobs:
            rows.append(Job(**job))

        Job.objects.insert(rows)

        fetch_store(state, logger)

        hours = set([job["hour"] for job in jobs])
        out = {}

        for hour in hours:
            out.update(run_jobs_of_nth_hour(hour, state))

        num_jobs = len(out)

        code_logs = {
            f"----job::{k}----\n{v}\n-------------------------------------\n"
            for k, v in out.items()
        }
        code_logs = "\n".join(code_logs)

        logger.code_logger.info(
            "Ran the jobs[%s] that were scheduled in the #11 and #12 hour bucket, outputs:\n\n%s",
            str(num_jobs),
            code_logs,
        )

        # assert for correct output
        for job in rows:
            assert out[str(job.id)] == job.deps[0] + "\n"
            job.delete()

    def test_execute_jobs(self):
        state = State()
        logger = Logger()

        rows = []
        jobs[-1]["hour"] = str(datetime.datetime.now().hour)

        for job in jobs:
            rows.append(Job(**job))

        Job.objects.insert(rows)
        job_id_last = str(rows[-1].id)

        fetch_store(state, logger)
        out = execute_jobs(state, logger)  # execute jobs in the current hour bucket

        assert job_id_last in out
        assert out[job_id_last] == rows[-1].deps[0] + "\n"

        for job in rows:
            job.delete()


class TestCodeObj:
    @classmethod
    def setup_class(cls):
        """setup any state specific to the execution of the given class."""
        setup_logging()
        logger = Logger()
        logger.root_logger = logging.getLogger("root")
        logger.pip_logger = logging.getLogger("pip")
        logger.code_logger = logging.getLogger("code")

    def test_correct_output(self):
        logger = Logger()
        src = "print('hello')"
        name = "test"

        # assert correct output
        code = Code(name, src)
        out = code.run_with_std_patch()
        logger.code_logger.info(f"\nSource:\n{src}\nOutput:\n{out}")
        assert "hello\n" == out

    def test_syntax_error_code_execution(self):
        logger = Logger()

        src = "print(;)"
        name = "test2"

        # check that the code with syntax errors does't break the flow of the script execution
        code = Code(name, src)
        out = code.run_with_std_patch()
        logger.code_logger.info(f"\nSource:\n{src}\nOutput:\n{out}")

    def test_exceptions_raised_inside_code_object(self):
        logger = Logger()

        src = "print(1/0)"
        name = "test3"

        # check that the exceptions raised inside the remote code after executing it, does't break the flow of the script execution
        code = Code(name, src)
        out = code.run_with_std_patch()
        logger.code_logger.info(f"\nSource:\n{src}\nOutput:\n{out}")
