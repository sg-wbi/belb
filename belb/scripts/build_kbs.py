#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Build all kbs available in BELB
"""

import argparse
import multiprocessing as mp

from loguru import logger

from belb.kbs import NAME_TO_KB_MODULE
from belb.resources import Kbs
from belb.utils import set_logging


def parse_args() -> argparse.Namespace:
    """
    Parse CLI
    """

    parser = argparse.ArgumentParser(
        description="Build all available BELB corpora")

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
        "--dbsnp",
        default=None,
        type=str,
        help="Directory where raw dbSNP data is stored",
    )
    parser.add_argument(
        "--umls",
        default=None,
        type=str,
        help="Directory where raw UMLS data is stored",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite data",
    )
    parser.add_argument(
        "--cores",
        type=int,
        default=min(mp.cpu_count(), 30),
        help="Available cores",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Log level to DEBUG",
    )

    return parser.parse_args()


def main():
    """
    Standalone
    """

    args = parse_args()
    args.skip_kb = False
    args.skip_history = False
    args.skip_database = False

    set_logging(
        logger=logger,
        directory=args.dir,
        logfile="kbs.log",
        level="DEBUG" if args.debug else "INFO",
    )

    ready = [
        Kbs.CTD_DISEASES.name,
        Kbs.CTD_CHEMICALS.name,
        Kbs.NCBI_TAXONOMY.name,
        Kbs.CELLOSAURUS.name,
        Kbs.UMLS.name,
        Kbs.NCBI_GENE.name,
        # Kbs.DBSNP.name,
    ]

    for kb in Kbs:
        if kb.name not in ready:
            continue

        if kb.name == Kbs.UMLS.name:
            if args.umls is None:
                logger.warning("No path specified for UMLS data. Skip...")
                continue
            args.data_dir = args.umls

        if kb.name == Kbs.DBSNP.name:
            if args.dbsnp is None:
                logger.warning("No path specified for dbSNP data. Skip...")
                continue
            args.data_dir = args.dbsnp

        NAME_TO_KB_MODULE[kb.name].main(args)


if __name__ == "__main__":
    main()
