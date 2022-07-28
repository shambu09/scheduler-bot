import multiprocessing
import os
import subprocess
import threading
from logging.config import dictConfig
from time import sleep
from typing import Iterable, Set

from dotenv import load_dotenv

load_dotenv()


def setup_logging() -> None:
    dictConfig(
        {
            "version": 1,
            "formatters": {
                "default": {
                    "format": "[%(asctime)s] %(name)s:%(levelname)s in %(module)s.%(funcName)s [%(lineno)s] : %(message)s",
                }
            },
            "handlers": {
                "stream": {
                    "level": "INFO",
                    "class": "logging.StreamHandler",
                    "formatter": "default",
                },
                "file": {
                    "level": "INFO",
                    "class": "logging.FileHandler",
                    "formatter": "default",
                    "filename": "logs/app.log",
                    "mode": "a",
                },
                "pip_handler": {
                    "level": "INFO",
                    "class": "logging.FileHandler",
                    "formatter": "default",
                    "filename": "logs/pip.log",
                    "mode": "a",
                },
                "code_handler": {
                    "level": "INFO",
                    "class": "logging.FileHandler",
                    "formatter": "default",
                    "filename": "logs/code.log",
                    "mode": "a",
                },
            },
            "loggers": {
                "root": {
                    "level": "DEBUG",
                    "propagate": False,
                    "handlers": ["stream", "file"],
                },
                "pip": {
                    "level": "DEBUG",
                    "propagate": False,
                    "handlers": ["pip_handler", "file", "stream"],
                },
                "code": {
                    "level": "DEBUG",
                    "propagate": False,
                    "handlers": ["code_handler", "file", "stream"],
                },
            },
        }
    )


def get_URI(db: str) -> str:
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    return f"mongodb+srv://{DB_USERNAME}:{DB_PASSWORD}@cluster0.rgd8u.mongodb.net/{db}?retryWrites=true&w=majority"


def install_deps(needed_deps: Iterable[str]) -> str:
    cmd = f'pip install {" ".join(needed_deps)}'

    # pip install needed deps
    p = subprocess.Popen(
        args=[cmd],
        stdout=subprocess.PIPE,
        shell=True,
    )

    out, err = p.communicate()
    return out.decode("utf-8")


def get_deps() -> Set[str]:
    p = subprocess.Popen(
        args=[f"pip freeze"],
        stdout=subprocess.PIPE,
        shell=True,
    )

    out, err = p.communicate()
    deps = out.decode("utf-8").split("\n")
    deps = set(filter(lambda x: x != "", deps))

    return deps


def run_timer():
    def _run():
        __t = 0
        while True:
            print(__t)
            sleep(1)
            __t += 1

    # threading.Thread(target=_run).start()
    multiprocessing.Process(target=_run).start()


if __name__ == "__main__":
    run_timer()
