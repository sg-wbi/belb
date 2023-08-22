#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Run corpus
"""
from loguru import logger

from ...utils import set_logging
from ..corpus import CorpusConverter
from .bioid import BioIdCorpusConfig, main

if __name__ == "__main__":
    parser = CorpusConverter.get_argument_parser(
        description=f"Create {BioIdCorpusConfig.resource.name} corpus"
    )
    args = parser.parse_args()

    set_logging(
        logger=logger,
        directory=args.dir,
        logfile=f"{BioIdCorpusConfig.resource.name}.log",
        level="DEBUG" if args.debug else "INFO",
    )
    main(args)
