#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Interfaces to CTD Diseases
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

CTD_DISEASES_COLUMNS = [
    "symbol",
    "identifier",
    "alternative_identifiers",
    "definition",
    "parent_identifiers",
    "tree_numbers",
    "parent_tree_numbers",
    "synonyms",
    "slim_mappings",
]


def parse_line(line: str) -> tuple[list[dict], list[str]]:
    """
    Convert line to KB entry
    """

    values = line.strip().split("\t")
    row = dict(zip(CTD_DISEASES_COLUMNS, values))

    entries = []

    identifier = row["identifier"]

    if row.get("symbol") is not None:
        entry = {
            "identifier": identifier,
            "name": row["symbol"],
            "description": "symbol",
        }
        entries.append(entry)

    synonyms = [s for s in row.get("synonyms", "").split("|") if s != ""]

    for synonym in synonyms:
        entry = {
            "identifier": identifier,
            "name": synonym,
            "description": "synonym",
        }
        entries.append(entry)

    original_identifiers = [identifier]

    original_identifiers += [
        i for i in row.get("alternative_identifiers", "").split("|") if i != ""
    ]

    return entries, original_identifiers


class CtdDiseasesKbConfig(BaseKbConfig):
    """
    Base config CTD Diseases
    """

    resource = Kbs.CTD_DISEASES
    history = False
    foreign_identifier = False
    citations = False
    string_identifier = True
    local = False


class CtdDiseasesKbParser(BaseKbParser):
    """
    Preprocess CTD Diseases KB
    """

    def populate_description_codes(self, description: Optional[str] = None):

        self.description_codes.update({"symbol": SYMBOL_CODE, "synonym": 1})

    def parse_entries(
        self, directory: str, cores: Optional[int] = None
    ) -> Iterator[Entry]:
        """
        Entries generator
        """

        file = os.path.join(directory, "CTD_diseases.tsv.gz")

        self.populate_description_codes()

        uid = 0
        int_id = 0

        with smart_open(file, encoding="utf-8") as infile:

            for line in infile:

                if line.startswith("#"):
                    continue

                entries, original_identifiers = parse_line(line=line)

                # empty entry
                if "|".join(original_identifiers) == "MESH:C":
                    continue

                for i in original_identifiers:
                    self.identifier_mapping[i] = int_id

                for entry in entries:
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

    schema = BelbKbSchema(db_config=args.db, kb_config=CtdDiseasesKbConfig())

    converter = KbConverter(
        directory=args.dir, schema=schema, parser=CtdDiseasesKbParser()
    )

    converter.to_belb(
        cores=args.cores,
        log_every_n_entries=10000,
        skip_kb=args.skip_kb,
        skip_history=args.skip_history,
        overwrite=args.overwrite,
    )

    kb = BelbKb(directory=args.dir, schema=schema, debug=args.debug)
    with kb as handle:
        handle.init_database(dedup=True)
