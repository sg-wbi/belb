#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Define base parser to convert corpus to BELB format
"""
import copy
from abc import ABCMeta, abstractmethod
from typing import Any, Optional

import wrapt

from belb.kbs.kb import BelbKb
from belb.preprocessing.data import Annotation, Entities, Example, Passage
from belb.resources import Resource
from belb.utils import (METADATA, AbstractBelbBaseConfig, FrozenError, StrEnum,
                        load_corpus_splits)


class Splits(StrEnum):
    """
    Container for splits
    """

    TRAIN = "train"
    DEV = "dev"
    TEST = "test"


class BaseBelbCorpusConfig(AbstractBelbBaseConfig):
    """
    Base configuration for corpus
    """

    resource = None
    local = None
    title_abstract = None
    pmc = None
    splits = None
    entity_type = None
    entity_types = None
    foreign_entity_types = None
    entity_type_map = None
    native_foreign_annotations = False
    add_foreign_annotations = False
    sentences = False
    mention_markers = False
    annotations_file = None
    subset = None
    subsets = None
    max_mentions = -1

    def __init__(
        self,
        sentences: bool = False,
        mention_markers: bool = False,
        entity_type: Optional[Entities] = None,
        add_foreign_annotations: Optional[bool] = None,
        foreign_entity_types: Optional[list] = None,
        native_foreign_annotations: Optional[bool] = None,
        annotations_file: Optional[str] = None,
        max_mentions: Optional[int] = None,
    ):

        kwargs = {
            "entity_type": entity_type,
            "sentences": sentences,
            "mention_markers": mention_markers,
            "add_foreign_annotations": add_foreign_annotations,
            "foreign_entity_types": foreign_entity_types,
            "annotations_file": annotations_file,
            "native_foreign_annotations": native_foreign_annotations,
            "max_mentions": max_mentions,
        }

        kwargs = {k: v for k, v in kwargs.items() if v is not None}

        for k, v in kwargs.items():
            super().__setattr__(k, v)

    def sanity_check(self):

        for key in [
            "resource",
            "local",
            "title_abstract",
            "pmc",
            "splits",
            "entity_type",
            "entity_types",
        ]:
            assert (
                getattr(self, key, None) is not None
            ), f"Corpus configuration must define attribute `{key}`"

        assert isinstance(
            self.resource, Resource
        ), "Attribute `resource` muse be an instance of `Resource`"

        assert all(
            isinstance(s, Splits) for s in self.splits
        ), "Attribute `split` must be a list of valid `Splits`"

        for key in ["local", "title_abstract", "pmc"]:
            assert isinstance(
                getattr(self, key, None), bool
            ), f"Attribute `{key}` muse be an instance of `bool`"

        assert (
            self.entity_type in self.entity_types
        ), f"Invalid `entity_type`: {self.entity_type}. Must be one of {self.entity_types}!"

        if len(self.entity_types) > 1:
            assert (
                self.entity_type_map is not None
            ), "With >1 entity types configuration must specify `entity_type_map`"

        if self.add_foreign_annotations:
            assert (
                self.foreign_entity_types is not None
            ), "With `add_foreign_annotations=True` attribute `foreign_entity_types` cannot be None"

        if self.subsets is not None:
            # null subset means everything
            if self.subset is not None:
                assert (
                    self.subset in self.subsets
                ), f"Subset `{self.subset}` must be one of `{tuple(self.subsets.keys())}`"

    def __setattr__(self, name: str, value: Any):
        raise FrozenError("Config object is immutable!")


def get_annotation(args: list, kwargs: dict):
    """
    Get annotation argument from args/kwargs
    """

    a = None

    for v in args:
        if isinstance(v, Annotation):
            a = v

    if a is None:
        for _, v in kwargs.items():
            if isinstance(v, Annotation):
                a = v

    assert a is not None, "Could not get argument containing `Annotation`!"

    return a


@wrapt.decorator
def qaqcmethod(wrapped, instance, args, kwargs):  # pylint: disable=unused-argument
    """
    Check if method modifies in-place annotation
    """

    original = copy.deepcopy(get_annotation(args, kwargs))

    wrapped(*args, **kwargs)

    processed = get_annotation(args, kwargs)

    handled = original != processed

    return handled


class BaseBelbCorpusParser(metaclass=ABCMeta):
    """
    Base class to parse corpus
    """

    def __init__(self, config: BaseBelbCorpusConfig):
        self.config = config
        self.splits: dict = {}
        if (METADATA / self.config.name).is_dir():
            # split are identical for all entity_types/subsets
            self.splits.update(load_corpus_splits(self.config.name))

    @abstractmethod
    def load_split(self, directory: str, split: str) -> list[Example]:
        """
        Load examples in split
        """

    @abstractmethod
    def parse_annotation_identifiers(self, original_identifiers: str):
        """
        Preprocess original annotation identifiers:
            - expand to List
            - set to `-1` those to be removed: e.g. out-of-kb for corpore w/ pre-computed identifier mapping
        """

    @qaqcmethod
    def handle_errors_annotation_offsets(
        self, eid: str, a: Annotation, p: Passage  # pylint: disable=unused-argument
    ):
        """Fix annotation offsets: return true if annotation had error"""

    @qaqcmethod
    def handle_errors_annotation_text(
        self, eid: str, a: Annotation, p: Passage  # pylint: disable=unused-argument
    ):
        """Fix annotation text: return true if annotation had error"""

    def on_before_load(
        self, directory: str, kb: BelbKb
    ):  # pylint: disable=unused-argument
        """
        Hook to perform operations before data is loaded
        """

    def on_after_load(self, data: dict, kb: BelbKb):  # pylint: disable=unused-argument
        """
        Hook to perform operations after data is loaded
        """
