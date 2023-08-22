#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Run kb
"""
from loguru import logger

from ...utils import set_logging
from ..kb import KbConverter
from .ctd_chemicals import CtdChemicalsKbConfig, main

if __name__ == "__main__":

    parser = KbConverter.get_argument_parser(
        description=f"Create {CtdChemicalsKbConfig.resource.name} KB"
    )
    args = parser.parse_args()

    set_logging(
        logger=logger,
        directory=args.dir,
        logfile=f"{CtdChemicalsKbConfig.resource.name}.log",
        level="DEBUG" if args.debug else "INFO",
    )
    main(args)
