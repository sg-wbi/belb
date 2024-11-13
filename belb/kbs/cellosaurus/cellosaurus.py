#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Interfaces to Cellosaurus
"""

import os
from argparse import Namespace
from collections import defaultdict
from typing import Iterator, Optional

# from belb.kbs.ncbi_taxonomy import NcbiTaxonomyKbConfig
from belb.kbs.kb import BelbKb, KbConverter
from belb.kbs.parser import BaseKbConfig, BaseKbParser
from belb.kbs.schema import BelbKbSchema
from belb.preprocessing.data import SYMBOL_CODE, Entry, HistoryEntry
from belb.resources import Kbs
from belb.utils import load_foreign_patch

CELLOSAURUS_FOREIGN_PATCH = load_foreign_patch(Kbs.CELLOSAURUS.name)


def get_attribute(row: dict) -> str:
    """
    Determine identifier attribute.
    It is either disease or cell category
    """

    if row.get("disease") is not None:
        # there can be multiple diseases: pick shortest
        attr_name = "disease"
        attr_value = min(e.split(";")[-1].strip() for e in row["disease"])

    else:
        attr_name = "category"
        attr_value = row["category"]

    return f"{attr_name}:{attr_value}"


def parse_row(row: dict) -> tuple[list[dict], list, dict]:
    """
    Extract data from row: entries and citations
    """

    entries: list = []
    citations: dict = {}

    identifier = row["identifer"]

    # NCBI_TaxID=9606; ! Homo sapiens
    species = [s.split(";")[0].replace("NCBI_TaxID=", "") for s in row["species"]]

    for sp in species:
        entry = {
            "identifier": identifier,
            "name": row["symbol"],
            "foreign_identifier": sp,
            "description": "symbol",
            "attribute": get_attribute(row),
        }
        entries.append(entry)

    if "synonyms" in row:
        synonyms = [s.strip() for s in row["synonyms"].split(";")]
        for sp in species:
            for syn in synonyms:
                entry = {
                    "identifier": identifier,
                    "name": syn,
                    "foreign_identifier": sp,
                    "description": "synonym",
                    "attribute": get_attribute(row),
                }
                entries.append(entry)

    original_identifiers = [identifier]
    if "alternative_identifiers" in row:
        original_identifiers += [
            i.strip() for i in row["alternative_identifiers"].split(";")
        ]

    if "references" in row:
        references = [r.strip() for ref in row["references"] for r in ref.split(";")]
        references = [
            r.replace("PubMed=", "") for r in references if r.startswith("PubMed")
        ]
        citations.update({r: original_identifiers for r in references})

    return entries, original_identifiers, citations


def update_row(key: str, value: str, item: dict):
    """
    Add elements to parsed row
    """

    if key == "ID":  # Once; starts an entry
        item["symbol"] = value
    elif key == "AC":  # Once
        item["identifer"] = value
    elif key == "AS":  # Optional; once
        item["alternative_identifiers"] = value
    elif key == "SY":  # Optional; once
        item["synonyms"] = value
    elif key == "OX":  # Once or more
        if "species" not in item:
            item["species"] = set()
        item["species"].add(value)
    elif key == "RX":  # Optional: once or more
        if "references" not in item:
            item["references"] = set()
        item["references"].add(value)
    elif key == "DI":
        if "disease" not in item:
            item["disease"] = set()
        item["disease"].add(value)
    elif key == "AG":
        item["donor-age"] = value
    elif key == "CA":
        item["category"] = value


def parse_file(path: str) -> Iterator[dict]:
    """
    Extract rows of data from `cellosaurus.txt`
    """

    item: dict = {}

    with open(path) as infile:
        for idx, line in enumerate(infile):
            if idx < 54:
                continue

            line = line.strip()

            if line == "//":
                yield item

                item.clear()

                continue

            field, value = line.split("   ")

            update_row(key=field, value=value, item=item)


def parse_history_file(path: str) -> Iterator[HistoryEntry]:
    """
    Extract discontinued identifiers
    """

    with open(path) as infile:
        for idx, line in enumerate(infile):
            if idx < 11:
                continue
            if line == "\n":
                continue

            old_identifier, _ = line.strip().split("  ")
            yield HistoryEntry(old_identifier=old_identifier, new_identifier="-1")


class CellosaurusKbConfig(BaseKbConfig):
    """
    Base config Cellosaurus

    select * from cellosaurus_foreign_identifiers where name is null;
    9790|
    1606683|
    2735276|
    """

    resource = Kbs.CELLOSAURUS
    history = True
    foreign_identifier = True
    foreign_kb = Kbs.NCBI_TAXONOMY.name
    string_identifier = True
    citations = True
    attribute = True
    local = False
    foreign_patch = CELLOSAURUS_FOREIGN_PATCH


class CellosaurusKbParser(BaseKbParser):
    """
    Interface to Cellosaurus KB
    """

    def populate_description_codes(self, description: Optional[str] = None):
        self.description_codes.update({"symbol": SYMBOL_CODE, "synonym": 1})

    def parse_entries(
        self, directory: str, cores: Optional[int] = None
    ) -> Iterator[Entry]:
        path = os.path.join(directory, "cellosaurus.txt")

        citations: dict = defaultdict(set)

        self.populate_description_codes()

        uid = 0
        int_id = 0

        for row in parse_file(path):
            entries, original_identifiers, entry_citations = parse_row(row)

            for i in original_identifiers:
                self.identifier_mapping[i] = int_id

            for pmid, identifiers in entry_citations.items():
                citations[pmid].update(identifiers)

            for entry in entries:
                entry["uid"] = uid
                entry["identifier"] = str(int_id)
                entry["description"] = self.description_codes[entry["description"]]
                self.foreign_identifiers.add(entry["foreign_identifier"])
                yield Entry(**entry)
                uid += 1

            int_id += 1

        self.citations.update({k: list(v) for k, v in citations.items() if len(v) > 0})

    def parse_history_entries(self, directory: str, cores: Optional[int] = None):
        path = os.path.join(directory, "cellosaurus_deleted_ACs.txt")

        for entry in parse_history_file(path):
            yield entry


def main(args: Namespace):
    """
    Standalone
    """

    schema = BelbKbSchema(db_config=args.db, kb_config=CellosaurusKbConfig())

    converter = KbConverter(
        directory=args.dir, schema=schema, parser=CellosaurusKbParser()
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
        # foreign_schema = BelbKbSchema(
        #     db_config=args.db, kb_config=NcbiTaxonomyKbConfig()
        # )
        # foreign_kb = BelbKb(directory=args.dir, schema=foreign_schema, debug=args.debug)
        # handle.update_foreign_identifiers(foreign_kb=foreign_kb)
