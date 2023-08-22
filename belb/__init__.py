#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Biomedical Entity Linking Benchmark
"""

import os
from typing import Optional, Union

from .corpora import (ENTITY_TO_CORPORA_NAMES, NAME_TO_CORPUS_CONFIG,
                      AutoBelbCorpus, AutoBelbCorpusConfig)
from .corpora.corpus import BelbCorpus
from .corpora.parser import BaseBelbCorpusConfig, Splits
from .kbs import ENTITY_TO_KB_NAME, AutoBelbKb
from .kbs.kb import BelbKb
from .kbs.parser import BaseKbConfig
from .kbs.query import Queries
from .kbs.schema import Tables
from .preprocessing.data import (IDENTIFIERS_CONNECTOR, INVALID_POSITION, NA,
                                 OBSOLETE_IDENTIFIER, SYMBOL_CODE, Annotation,
                                 Entities, Example, Passage)
from .utils import compute_hexdigest


def assert_exists(
    path: str,
    config: Union[BaseBelbCorpusConfig, BaseKbConfig],
):
    """
    Exists
    """

    if not os.path.exists(path):
        raise RuntimeError(
            f"Resource `{config.name}`  was never created! Please check the README.md on how to initialize this resource!"
        )


class BelbPath(str):
    """
    Generate str object: path belb resource
    """

    def __new__(
        cls,
        directory: str,
        config: Union[BaseBelbCorpusConfig, BaseKbConfig],
        hasher: str = "md5",
        lm: Optional[str] = None,
    ):

        parts = [directory]

        if lm is not None:
            parts.append("tokenized")
            lm_hexdigest = compute_hexdigest((lm,), hasher)
        else:
            parts.append("processed")

        if isinstance(config, BaseBelbCorpusConfig):
            corpus_hexdigest = config.to_hexdigest(hasher)
            parts.extend(["corpora", config.name])
            if lm is not None:
                parts.extend([lm_hexdigest, corpus_hexdigest])
            else:
                parts.append(corpus_hexdigest)
        elif isinstance(config, BaseKbConfig):
            parts.append(config.name)
            if lm is not None:
                if config.subset is not None:
                    parts.append(config.subset)
                parts.append(lm_hexdigest)

        path = os.path.join(*parts)

        assert_exists(path=path, config=config)

        return str.__new__(cls, path)
