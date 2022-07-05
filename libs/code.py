"""
Code object and other helper methods for remote code execution.
https://github.com/shambu09/remote-code-execution
"""

import importlib
import io
import sys
from dataclasses import dataclass, field
from types import ModuleType
from typing import Any, List, Optional


def __save_module_to_file(src: str, module_name: str) -> None:
    """
    Save the code to a file.
    :param str src: Code to be saved.
    :param str module_name: Name of the module.
    :return: None
    :rtype: NoneType
    """

    with open(module_name + ".py", "w") as f:
        f.write(src)


def import_file(mod_name: str) -> ModuleType:
    """
    Import a file.
    :param str mod_name: Name of the module.
    :return: Module.
    :rtype: ModuleType
    """

    module = importlib.import_module(mod_name)
    return module


def import_dmod(name: str, src: str) -> ModuleType:
    """
    Import dynamically generated code as module.
    :param str name: Name of the module.
    :param str src: Code to be imported as module.
    :return: Module.
    :rtype: ModuleType
    """
    spec = importlib.util.spec_from_loader(name, loader=None)
    module = None

    try:
        module = importlib.util.module_from_spec(spec)
        exec(src, module.__dict__)

    except Exception as E:
        module = importlib.util.module_from_spec(spec)
        _src = functionalise_src(f'print("""{repr(E)}""")')
        exec(_src, module.__dict__)

    return module


class PropertyMissingException(Exception):
    """
    Exception for missing properities.
    """

    pass


class CodeMissingException(Exception):
    """
    Exception for missing source code.
    """

    pass


class ProxyStream:
    """
    Proxy stream object for temporary storage.
    """

    value: str = ""

    @classmethod
    def write(cls, string: str) -> None:
        cls.value += string

    @classmethod
    def flush(cls) -> None:
        pass


class PatchStd:
    """
    Context manager for monkey-patching stdout.
    """

    def __init__(self) -> None:
        self._out = sys.stdout
        self.out = io.StringIO()
        self.value = ""

    def _print(self, *args) -> None:
        print(*args, file=self._out)

    def __enter__(self) -> "PatchStd":
        sys.stdout = self.out
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        sys.stdout = self._out
        self.value = self.out.getvalue()
        del self.out


def validate_properties(module: ModuleType, properties: List[str]) -> None:
    """
    Validate properties.
    :param ModuleType module: Module or code class.
    :return: None
    """
    for property in properties:
        if not hasattr(module, property):
            raise PropertyMissingException(f"Property {property} is missing.")


def functionalise_src(src: str) -> str:
    """
    Functionalise the source code.
    :param str src: Source code.
    :return: Functionalised source code.
    :rtype: str
    """
    src = src.replace("\n", "\n\t")
    return f"def i__run__():\n\t" + src


@dataclass(frozen=True)
class Code:
    """
    //:param str uid: Unique identifier.
    :param str name: Name of the module.
    :param str src: Code to be imported as module.
    :param ModuleType module: Module oobject of the code.
    """

    # //uid: str = field(default="")
    name: str = field(default="")
    src: str = field(default="", repr=False)
    lib: Optional[ModuleType] = field(default=None, repr=False)

    def __post_init__(self) -> None:
        """
        Post init.
        :return: None
        :rtype: NoneType
        """
        if self.src != "":
            object.__setattr__(
                self, "lib", import_dmod(self.name, functionalise_src(self.src))
            )
        else:
            raise CodeMissingException(f"Source code is missing.")

        validate_properties(self.lib, ["i__run__"])

    def run_with_std_patch(self) -> str:
        try:
            with PatchStd() as std:
                self.lib.i__run__()
            return std.value

        except Exception as E:
            return repr(E)


if __name__ == "__main__":
    src = """
def r_lambda(*args, **kwargs):
    print("Hello")
r_lambda()
"""
    module_name = "test_module"

    module = Code(module_name, src)
    print(module.run_with_std_patch())
