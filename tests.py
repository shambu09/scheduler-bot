import datetime
import logging

from mongoengine import *

from add_jobs import add_jobs
from libs.code import Code
from libs.db import Job
from libs.store import Store
from main import CodeJob, Logger, State, cronify, fetch_store
from utils import get_deps, get_URI, setup_logging

jobs = [
    {
        "cron": {"hour": "11"},
        "deps": ["flask"],
        "code": f"import flask\nprint('flask')",
    },
    {
        "cron": {"hour": "11"},
        "deps": ["requests"],
        "code": f"import requests\nprint('requests')",
    },
    {
        "cron": {"hour": "12"},
        "deps": ["Scrapy==2.6.1"],
        "code": f"import flask\nprint('Scrapy==2.6.1')",
    },
    {
        "cron": {"hour": "13"},
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
        state.store = Store()
        logger.root_logger.info("Created in-memory store")
        logger.root_logger.info(state.store)

        state.deps = get_deps()
        logger.pip_logger.info("Indexed deps: %s", state.deps)

    def test_cronify(self):
        def check(_jobs):
            for job_1 in _jobs:
                for job_2 in _jobs:
                    if job_1["cron"] == job_2["cron"]:
                        assert cronify(job_1["cron"]) == cronify(job_2["cron"])
                    else:
                        assert cronify(job_1["cron"]) != cronify(job_2["cron"])

        rows = []
        for job in jobs:
            rows.append(Job(**job))

        Job.objects.insert(rows)

        check(jobs)
        check(rows)

        for job in rows:
            job.delete()

    def test_add_jobs(self):
        rows = add_jobs(jobs)
        _rows = Job.objects(sync=False)

        for job in rows:
            assert job in _rows
            job.delete()

    def test_fetch_store(self):
        state = State()
        logger = Logger()
        cron = jobs[2]["cron"]
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
            f"Before fetch_store: code present in #{cronify(cron)} bucket = {any([code == code_obj.src for code_obj in state.store[cronify(cron)]])}"
        )

        fetch_store(state, logger)

        logger.pip_logger.info("After fetch_store: Deps installed: %s", str(state.deps))

        logger.pip_logger.info("After fetch_store: Store: %s", str(state.store))

        logger.pip_logger.info(
            f"After fetch_store: {dep} installed = {dep in state.deps}"
        )

        logger.pip_logger.info(
            f"After fetch_store: code present in #{cronify(cron)} bucket = {any([code == code_obj.src for code_obj in state.store[cronify(cron)]])}"
        )

        job.delete()
        assert dep in state.deps
        assert any([code == code_obj.src for code_obj in state.store[cronify(cron)]])

    def test_run_jobs_of_nth_hour(self):
        state = State()
        logger = Logger()

        rows = []
        for job in jobs:
            rows.append(Job(**job))

        Job.objects.insert(rows)

        fetch_store(state, logger)

        cron_bucket = set([cronify(job["cron"]) for job in jobs])
        out = {}

        for bucket in cron_bucket:
            for code_obj in state.store[bucket]:
                out.update(CodeJob(code_obj, logger)())

        num_jobs = len(out)

        # assert for correct output
        for job in rows:
            assert out[str(job.id)] == job.deps[0] + "\n"
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
