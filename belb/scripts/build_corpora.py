#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Build all corpora available in BELB
"""
import argparse
import itertools

from loguru import logger

from belb.corpora import NAME_TO_CORPUS_MODULE, AutoBelbCorpusConfig
from belb.corpora.parser import BaseBelbCorpusConfig
from belb.resources import Corpora
from belb.utils import set_logging


def parse_args() -> argparse.Namespace:
    """
    Parse CLI
    """

    parser = argparse.ArgumentParser(description="Build all available BELB corpora")

    parser.add_argument(
        "--dir", required=True, type=str, help="Directory where BELB data is stored"
    )
    parser.add_argument(
        "--db",
        required=True,
        type=str,
        help="Database configuration",
    )
    parser.add_argument(
        "--pubtator",
        type=str,
        default=None,
        help="Path to local PubTator sqlite DB (built from `bioconcepts2pubtator.offsets`)",
    )
    parser.add_argument(
        "--annotations_file",
        type=str,
        default=None,
        help="Path to file containing corpus annotations (hexdigests)",
    )
    parser.add_argument(
        "--max_mentions",
        type=int,
        default=-1,
        help="Upsample each example to have this max amount of mentions",
    )
    parser.add_argument("--debug", action="store_true", help="Log level to DEBUG")

    return parser.parse_args()


def has_foreign_annotations(config: BaseBelbCorpusConfig):
    """
    Check if corpus has foreign annotations
    """

    return config.foreign_entity_types is not None


def main():
    """
    Standalone
    """

    args = parse_args()
    args.exclude_foreign = False

    set_logging(
        logger=logger,
        directory=args.dir,
        logfile="corpora.log",
        level="DEBUG" if args.debug else "INFO",
    )

    ready = [
        Corpora.GNORMPLUS.name,
        Corpora.NLM_GENE.name,
        Corpora.NCBI_DISEASE.name,
        Corpora.BC5CDR.name,
        Corpora.NLM_CHEM.name,
        Corpora.LINNAEUS.name,
        Corpora.S800.name,
        Corpora.BIOID.name,
        Corpora.MEDMENTIONS.name,
        # Corpora.SNP.name,
        # Corpora.OSIRIS.name,
        # Corpora.TMVAR.name,
    ]

    for resource in Corpora:

        if resource.name not in ready:
            continue

        config = AutoBelbCorpusConfig.from_name(resource.name)

        keys = ["sentences", "markers"]
        if has_foreign_annotations(config):
            keys.append("exclude_foreign")

        for values in itertools.product([True, False], repeat=len(keys)):

            for k, v in zip(keys, values):
                setattr(args, k, v)

            NAME_TO_CORPUS_MODULE[resource.name].main(args)


if __name__ == "__main__":
    main()
