#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BELB utilities
"""

import hashlib
import importlib.resources as pkg_resources
import inspect
import json
import os
import sys
from abc import abstractmethod
from dataclasses import asdict, is_dataclass
from enum import Enum, EnumMeta
from importlib.abc import Traversable
from typing import Iterable, Optional

import pandas as pd
from loguru import logger as Logger
from omegaconf import OmegaConf

from .chunkize import chunkize  # noqa: F401
from .download import download  # noqa: F401

METADATA: Traversable = pkg_resources.files("belb") / "metadata"


def remove_quotes(string: str) -> str:
    """Remove quotes from string (it messes up TSV parsing)"""

    return string.replace('"', "").replace("'", "")


def is_dataclass_instance(obj) -> bool:
    """
    https://docs.python.org/3/library/dataclasses.html#dataclasses.is_dataclass
    """
    return is_dataclass(obj) and not isinstance(obj, type)


def load_foreign_patch(name: str) -> dict:
    """
    Load `foreign_patcth`: name for foreign identifiers not in foreign_kb,
    i.e. discontinued ones or errors
    """

    path = METADATA / name / "foreign_patch.json"

    with path.open() as infile:
        subsets = json.load(infile)

    return subsets


def load_kb_subsets(name: str) -> dict:
    """
    Load kb subsets
    """

    path = METADATA / name / "subsets.json"

    with path.open() as infile:
        subsets = json.load(infile)

    return subsets


def load_corpus_splits(name: str):
    """
    Load splits of given corpus
    """

    path = METADATA / name / "splits.json"

    splits: dict = {}

    if path.is_file():
        with path.open() as infile:
            splits = json.load(infile)

    return splits


def load_zeroshot() -> pd.DataFrame:
    path = METADATA / "zeroshot.csv"
    return pd.read_csv(path, sep="\t")


def load_homonyms() -> pd.DataFrame:
    path = METADATA / "homonyms.csv"
    return pd.read_csv(path, sep="\t")


def load_stratified() -> pd.DataFrame:
    path = METADATA / "stratified.csv"
    return pd.read_csv(path, sep="\t")


def load_manual_notinkb_history(name: str) -> dict:
    """
    Load manually defined mapping for not-in-kb identifiers
    """

    path = METADATA / name / "notinkb_identifiers_history.yaml"

    notinkb_identifiers_history: dict = {}

    if path.is_file():
        notinkb_identifiers_history.update(
            OmegaConf.to_container(OmegaConf.load(str(path)))
        )

    return notinkb_identifiers_history


class MetaEnum(EnumMeta):
    """
    https://stackoverflow.com/questions/63335753/how-to-check-if-string-exists-in-enum-of-strings/63336176

    >>> 2.3 in Stuff
    False

    >>> 'zero' in Stuff
    False
    """

    def __contains__(cls, item):
        try:
            cls(item)  # pylint: disable=E
        except ValueError:
            return False
        return True


class StrEnum(str, Enum, metaclass=MetaEnum):
    """
    String Enum

    >>> class Foo(Enum):
           TEST = 'test'

    >>> print(Foo.TEST == "test")
    False

    >>> class Bar(StrEnum):
           TEST = 'test'

    >>> print(Bar.TEST == "test")
    True
    """

    def __str__(self) -> str:
        return self.value

    def __repr__(self):
        return str(self)


class LogLevel(StrEnum):
    """
    Logging severity
    """

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


def set_logging(logger: Logger, directory: str, logfile: str, level: str):
    """
    Add logging to file
    """

    assert level in LogLevel

    # set logging level
    logger.remove()
    logger.add(sys.stderr, level=level)

    # set logging file
    logdir = os.path.join(directory, "logs")
    os.makedirs(logdir, exist_ok=True)
    logger.add(os.path.join(logdir, logfile), mode="a", rotation="1 week")


class CompressedFileWriter:
    """
    Mixin with helper methods to write in `wb` mode
    """

    def get_line_from_tuple(self, values: tuple) -> bytes:
        """
        Get encoded line
        """

        line = "\t".join(str(v) for v in values)

        line = f"{line}\n"

        return line.encode("utf-8")


def compute_hexdigest(message: Iterable, hasher: str = "md5") -> str:
    """
    Compute hexdigest
    """

    hash_func = getattr(hashlib, hasher, None)

    if hash_func is None:
        raise ValueError(f"Hash function `{hasher}` not found in hashlib!")

    h = hash_func()

    if isinstance(message, str):
        h.update(message.encode())
    else:
        for e in message:
            h.update(str(e).encode())

    return h.hexdigest()


def load_json(path: str, kwargs: Optional[dict] = None) -> dict:
    """
    Load JSON file into dict.
    You can pass kwargs to `json.load` and `open` via a nested dict:
        {"json": {"encoding": "utf-8"}, "open": {"encoding": "latin-w"}}
    Anything else will be ignored.
    """

    if kwargs is not None:
        assert isinstance(kwargs, dict), "Extra arguments must be a dictionary!"

    open_kwargs = kwargs.get("open", {}) if kwargs is not None else {}
    json_kwargs = kwargs.get("json", {}) if kwargs is not None else {}

    path = os.path.expanduser(path)

    with open(str(path), **open_kwargs) as infile:
        json_file = json.load(infile, **json_kwargs)

    return json_file


def save_json(path: str, item: dict, kwargs: Optional[dict] = None):
    """
    Save dict into JSON file.
    You can pass kwargs to `json.load` and `open` via a nested dict:
        {"json": {"indent": 1}, "open": {"encoding": "latin-w"}}
    Anything else will be ignored.
    """

    if kwargs is not None:
        assert isinstance(kwargs, dict), "Extra arguments must be a dictionary!"

    open_kwargs = kwargs.get("open", {}) if kwargs is not None else {}
    json_kwargs = kwargs.get("json", {}) if kwargs is not None else {}

    path = os.path.expanduser(path)

    with open(str(path), mode="w", **open_kwargs) as outfile:
        json.dump(item, outfile, **json_kwargs)


class FrozenError(Exception):
    """Object should not be modified"""


class MetaBelbBaseConfig(type):
    """
    Meta class to call `sanity_check` after instantiation
    """

    def __call__(cls, *args, **kwargs):
        """Called when you call Foo(*args, **kwargs)"""
        obj = type.__call__(cls, *args, **kwargs)
        obj.sanity_check()
        return obj


class AbstractBelbBaseConfig(metaclass=MetaBelbBaseConfig):
    """
    Base configuration
    """

    @property
    def name(self):
        """
        Shortcut to name
        """
        return self.resource.name

    @abstractmethod
    def sanity_check(self):
        """
        Check all attributes (class/instance) are valid
        """

    def to_dict(self) -> dict:
        """
        Convert config to dictionary: must include class attributes
        """

        attributes = inspect.getmembers(self, lambda a: not inspect.isroutine(a))

        config = dict(
            a for a in attributes if not (a[0].startswith("__") and a[0].endswith("__"))
        )

        # handle dataclasses
        for config_key, config_value in list(config.items())[:]:
            if is_dataclass_instance(config_value):
                for k, v in asdict(config.pop(config_key)).items():
                    config[k] = v

        return config

    def to_hexdigest(self, hasher: str = "md5") -> str:
        """
        Convert to hash digest
        """

        config = json.dumps(
            {k: str(v) for k, v in self.to_dict().items()},
            sort_keys=True,
            ensure_ascii=True,
            default=str,
        )

        return compute_hexdigest(message=config, hasher=hasher)

    def to_omegaconf(self):
        """
        Convert to OmegaConf object
        """

        return OmegaConf.create(self.to_dict())

    def save(self, directory: str):
        """
        Save as YAML
        """
        OmegaConf.save(self.to_omegaconf(), os.path.join(directory, "conf.yaml"))

    def __str__(self):
        return str(OmegaConf.to_yaml(self.to_omegaconf()))

    def __repr__(self):
        return str(OmegaConf.to_yaml(self.to_omegaconf()))


def chunkize_list(a: list, n: int):
    """
    Divide list in `n` (almost) equally sized parts
    """

    if not len(a) >= n:
        raise ValueError(f"Cannot split a list of length {len(a)} into {n} chunks!")
    # n = min(n, len(a))  # don't create empty buckets
    k, m = divmod(len(a), n)
    chunks = (a[i * k + min(i, m) : (i + 1) * k + min(i + 1, m)] for i in range(n))
    return chunks
