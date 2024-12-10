#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Interfaces to CTD Chemicals
"""

import os
from argparse import Namespace
from typing import Iterator, Optional

from smart_open import smart_open

from belb.kbs.kb import BelbKb, KbConverter
from belb.kbs.parser import BaseKbConfig, BaseKbParser
from belb.kbs.schema import BelbKbSchema
from belb.preprocessing.data import SYMBOL_CODE, Entry
from belb.resources import Kbs

CTD_CHEMICALS_COLUMNS = [
    "symbol",
    "identifier",
    "casrn",
    "definition",
    "parent_identifiers",
    "tree_numbers",
    "parent_tree_numbers",
    "synonyms",
]


def parse_line(line: str) -> list[dict]:
    """
    Convert line to Entry
    """

    values = line.strip().split("\t")
    row: dict = dict(zip(CTD_CHEMICALS_COLUMNS, values))

    identifier = row["identifier"]

    entries = []

    if row.get("symbol") is not None:
        entry = {
            "identifier": identifier,
            "name": row["symbol"],
            "description": "symbol",
        }
        entries.append(entry)

    synonyms = [s for s in row.get("synonyms", "").split("|") if s != ""]

    for synonym in synonyms:

        if synonym == row.get("symbol"):
            continue

        entry = {
            "identifier": identifier,
            "name": synonym,
            "description": "synonym",
        }
        entries.append(entry)

    return entries


class CtdChemicalsKbConfig(BaseKbConfig):
    """
    Base config CTD Chemical
    """

    resource = Kbs.CTD_CHEMICALS
    history = False
    foreign_identifier = False
    string_identifier = True
    citations = False
    local = False


class CtdChemicalsKbParser(BaseKbParser):
    """
    Interface to CTD Chemical KB
    """

    def populate_description_codes(self, description: Optional[str] = None):

        self.description_codes.update({"symbol": SYMBOL_CODE, "synonym": 1})

    def parse_entries(
        self, directory: str, cores: Optional[int] = None
    ) -> Iterator[Entry]:
        """
        Parse entries
        """

        file = os.path.join(directory, "CTD_chemicals.tsv.gz")

        self.populate_description_codes()

        uid = 0
        int_id = 0

        with smart_open(file, encoding="utf-8") as infile:

            for line in infile:

                if line.startswith("#"):
                    continue

                entries = parse_line(line)

                for entry in entries:

                    # `Definition`
                    # The MeSH ID below was used by MeSH when this chemical was part of the MeSH controlled vocabulary.
                    if entry["identifier"] == "MESH:D013749":

                        continue

                    self.identifier_mapping[entry["identifier"]] = int_id

                    entry["uid"] = uid
                    entry["identifier"] = str(int_id)
                    entry["description"] = self.description_codes[entry["description"]]

                    yield Entry(**entry)

                    uid += 1

                int_id += 1


def main(args: Namespace):
    """
    Standalone
    """
    schema = BelbKbSchema(db_config=args.db, kb_config=CtdChemicalsKbConfig())

    converter = KbConverter(
        directory=args.dir, schema=schema, parser=CtdChemicalsKbParser()
    )

    converter.to_belb(
        cores=args.cores,
        log_every_n_entries=100000,
        skip_kb=args.skip_kb,
        skip_history=args.skip_history,
        overwrite=args.overwrite,
    )

    kb = BelbKb(directory=args.dir, schema=schema, debug=args.debug)
    with kb as handle:
        handle.init_database(dedup=True)
