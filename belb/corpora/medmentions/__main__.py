#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Run corpus
"""
from loguru import logger

from ...utils import set_logging
from ..corpus import CorpusConverter
from .medmentions import MedMentionsCorpusConfig, main

if __name__ == "__main__":
    parser = CorpusConverter.get_argument_parser(
        description=f"Create {MedMentionsCorpusConfig.resource.name} corpus"
    )
    args = parser.parse_args()

    set_logging(
        logger=logger,
        directory=args.dir,
        logfile=f"{MedMentionsCorpusConfig.resource.name}.log",
        level="DEBUG" if args.debug else "INFO",
    )

    main(args)
