#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Interfaces to NCBI Taxonomy
"""
import multiprocessing as mp
import os
import tarfile
from argparse import Namespace
from collections import OrderedDict
from typing import Iterator, Optional

from belb.kbs.kb import BelbKb, KbConverter
from belb.kbs.parser import BaseKbConfig, BaseKbParser
from belb.kbs.schema import BelbKbSchema
from belb.preprocessing.data import (OBSOLETE_IDENTIFIER, SYMBOL_CODE, Entry,
                                     HistoryEntry)
from belb.resources import Kbs
from belb.utils import chunkize

NCBI_TAXONOMY_SYMBOL = "scientific name"
NCBI_TAXONOMY_SYNSET = [
    "genbank common name",
    "common name",
    "scientific name",
    "equivalent name",
    "synonym",
    "acronym",
    "blast name",
    "genbank",
    "genbank synonym",
    "genbank acronym",
    "includes",
    "type material",
    "authority",
    "in-part",
]

NCBI_TAXONOMY_DESCRIPTION_CODES = OrderedDict(
    [
        (f, i if f != NCBI_TAXONOMY_SYMBOL else SYMBOL_CODE)
        for i, f in enumerate(NCBI_TAXONOMY_SYNSET, start=1)
    ]
)


def parse_line(line: str) -> dict:
    """
    Extract fields from line
    """

    elements = [e.strip() for e in line.strip().split("|")]
    identifier = elements[0]
    name = elements[1]
    genus = elements[2] if elements[2] != "" else None
    field = elements[3]  # synonym type
    if genus is not None:
        name = genus

    entry = {"identifier": identifier, "name": name, "description": field}

    return entry


class NcbiTaxonomyKbConfig(BaseKbConfig):
    """
    Default config for NCBI Gene
    """

    resource = Kbs.NCBI_TAXONOMY
    history = True
    foreign_identifier = False
    string_identifier = False
    citations = False
    local = False


class NcbiTaxonomyKbParser(BaseKbParser):
    """
    Preprocess NCBI Taxonomy KB
    """

    def populate_description_codes(self, description: Optional[str] = None):
        self.description_codes = NCBI_TAXONOMY_DESCRIPTION_CODES

    def stream_lines_from_archive(self, directory: str, file_name: str):
        """
        Extract from archive only file w/ names
        """

        archive_name = "taxdump.tar.gz"
        with tarfile.open(os.path.join(directory, archive_name)) as archive:
            member = archive.getmember(file_name)
            fp = archive.extractfile(member)
            assert (
                fp is not None
            ), f"File `{file_name}` not found in archive `{archive}`!"
            for line in fp.readlines():
                yield line.decode("utf-8")

    def parse_entries(
        self, directory: str, cores: Optional[int] = None
    ) -> Iterator[Entry]:
        """
        Parse NCBI Taxonomy file
        """

        cores = cores if cores is not None else 2
        batch_size = 10000
        chunksize = batch_size * cores

        self.populate_description_codes()

        uid = 0

        with mp.Pool(cores) as pool:
            for lines in chunkize(
                self.stream_lines_from_archive(
                    directory=directory, file_name="names.dmp"
                ),
                chunksize=chunksize,
            ):
                for entry in pool.imap(parse_line, lines, batch_size):
                    if entry["name"] in ["all", "root"]:
                        continue

                    if entry["description"] not in NCBI_TAXONOMY_SYNSET:
                        raise ValueError(f"Unknown `description` in `{entry}`!")

                    yield Entry(
                        uid=uid,
                        identifier=entry["identifier"],
                        name=entry["name"],
                        description=self.description_codes[entry["description"]],
                    )
                    uid += 1

    def parse_history_entries(self, directory: str, cores: Optional[int] = None):
        """
        Generate history entries of identifiers
        """

        for file_name in ["delnodes.dmp", "merged.dmp"]:
            for line in self.stream_lines_from_archive(
                directory=directory, file_name=file_name
            ):
                elems = [e.strip() for e in line.split("|")]
                elems = [e for e in elems if e != ""]

                if len(elems) == 1:
                    old_identifier = elems[0]
                    new_identifier = OBSOLETE_IDENTIFIER
                elif len(elems) == 2:
                    old_identifier = elems[0]
                    new_identifier = elems[1]

                entry = HistoryEntry(
                    old_identifier=old_identifier, new_identifier=new_identifier
                )

                yield entry


def main(args: Namespace):
    """
    Standalone
    """
    schema = BelbKbSchema(db_config=args.db, kb_config=NcbiTaxonomyKbConfig())

    converter = KbConverter(
        directory=args.dir, schema=schema, parser=NcbiTaxonomyKbParser()
    )

    converter.to_belb(
        cores=args.cores,
        log_every_n_entries=int(1e6),
        skip_kb=args.skip_kb,
        skip_history=args.skip_history,
        overwrite=args.overwrite,
    )

    kb = BelbKb(directory=args.dir, schema=schema, debug=args.debug)
    with kb as handle:
        handle.init_database(dedup=True)
