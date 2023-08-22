#!/usr/bin/env pythona
# -*- coding: utf-8 -*-
"""
Base interface to KB preprocessing
"""

from abc import ABCMeta, abstractmethod
from typing import Any, Iterator, Optional

from belb.preprocessing.data import Entry
from belb.resources import Resource
from belb.utils import AbstractBelbBaseConfig, FrozenError


class BaseKbConfig(AbstractBelbBaseConfig):
    """
    Base configuration for KB
    """

    resource = None
    history = None
    foreign_identifier = None
    string_identifier = None
    attribute = None
    citations = None
    local = None
    foreign_kb = None
    data_dir = None
    subset = None
    subsets = None
    foreign_patch = {}

    def __init__(self, data_dir: Optional[str] = None, subset: Optional[str] = None):
        # we use super because config must be immutable!
        super().__setattr__("data_dir", data_dir)
        super().__setattr__("subset", subset)

    @property
    def name(self):
        """
        Shortcut to name
        """
        return self.resource.name

    def sanity_check(self):
        """
        Configuraion sanity check
        """

        assert isinstance(
            self.resource, Resource
        ), "Attribute `resource` muse be an instance of `Resource`"

        for key in [
            "local",
            "history",
            "foreign_identifier",
            "citations",
            "string_identifier",
        ]:
            assert isinstance(
                getattr(self, key, None), bool
            ), f"Attribute `{key}` muse be an instance of `bool`"

        if self.foreign_identifier:
            assert (
                self.foreign_kb is not None
            ), "KB has contains foreign identifiers: need to specify `foreign_kb`"

        if self.subsets is not None:
            # null subset means everything
            if self.subset is not None:
                assert (
                    self.subset in self.subsets
                ), f"Subset `{self.subset}` must be one of `{tuple(self.subsets.keys())}`"

    def __setattr__(self, name: str, value: Any):
        raise FrozenError("Corpus config object is immutable!")


class BaseKbParser(metaclass=ABCMeta):
    """
    Base parser for KB
    """

    def __init__(self):
        self.description_codes = {}
        self.identifier_mapping = {}
        self.citations = {}
        self.foreign_identifiers = set()

    @abstractmethod
    def populate_description_codes(self, description: Optional[str] = None):
        """
        Create/populate incrementally description codes.
        This should contain a mapping from a `description` (e.g. symbol, preferred name, abbreviation)
        to a unique code (int). This is done to save space.
        """

    @abstractmethod
    def parse_entries(
        self, directory: str, cores: Optional[int] = None
    ) -> Iterator[Entry]:
        """
        Generate entries to populate kb table
        """

    def parse_history_entries(
        self,
        directory: str,  # pylint: disable=unused-argument
        cores: Optional[int] = None,  # pylint: disable=unused-argument
    ) -> Iterator[Entry]:
        """
        Generate entries to populate kb table
        """

        return iter([])

    @property
    def name(self) -> str:
        """
        Shortcut to class name
        """
        return self.__class__.__name__
