#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Run kb
"""
from loguru import logger

from ...utils import set_logging
from ..kb import KbConverter
from .ncbi_gene import NcbiGeneKbConfig, main

if __name__ == "__main__":

    parser = KbConverter.get_argument_parser(
        description=f"Create {NcbiGeneKbConfig.resource.name} KB"
    )
    args = parser.parse_args()

    set_logging(
        logger=logger,
        directory=args.dir,
        logfile=f"{NcbiGeneKbConfig.resource.name}.log",
        level="DEBUG" if args.debug else "INFO",
    )
    main(args)
