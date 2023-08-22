#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Interfaces to UMLS
Adapted from: https://github.com/cambridgeltl/sapbert/blob/main/training_data/generate_pretraining_data.ipynb
"""
import os
from argparse import Namespace
from typing import Iterator, Optional

from loguru import logger

from belb.kbs.kb import BelbKb, KbConverter
from belb.kbs.parser import BaseKbConfig, BaseKbParser
from belb.kbs.schema import BelbKbSchema
from belb.preprocessing.data import (OBSOLETE_IDENTIFIER, SYMBOL_CODE, Entry,
                                     HistoryEntry)
from belb.resources import Kbs

# NOTE: see Table 1 here for description: https://www.ncbi.nlm.nih.gov/books/NBK9685/#ch03.sec3.3.4
MRCONSO_COLUMNS = [
    "CUI",
    "LAT",
    "TS",
    "LUI",
    "STT",
    "SUI",
    "ISPREF",
    "AUI",
    "SAUI",
    "SCUI",
    "SDUI",
    "SAB",
    "TTY",
    "CODE",
    "STR",
    "SRL",
    "CVF",
]

# https://www.ncbi.nlm.nih.gov/books/NBK9685/table/ch03.T.retired_cui_mapping_file_mrcui_rr/?report=objectonly
MRCUI_COLUMNS = [
    "CUI1",
    "VER",
    "REL",
    "RELA",
    "MAPREASON",
    "CUI2",
    "MAPIN",
]

UMLS_SYMBOL = "PN"


def parse_file(path: str) -> tuple[dict, dict]:
    """
    Parse main file UMLS file `MRCONSO.RRF`.

    Extract:
        - `cui -> synstet`: mapping from CUI to all associated names
        - `xrefs` -> `cui -> source_db_ids` : mappings from CUI to identifiers in original dbs (e.g. MeSH)
    """

    logger.info("Start loading data from `{}`...", path)

    cui_to_names: dict = {}
    xrefs: dict = {}

    xref_code_columns = ["SAUI", "SCUI", "SDUI", "CODE"]

    with open(path) as infile:
        for line in infile:
            values = line.strip("\n").split("|")
            row = dict(zip(MRCONSO_COLUMNS, values))
            cui = row["CUI"]
            lang = row["LAT"]
            name = row["STR"]
            xref_db = row["SAB"]
            xref_codes = set(row[f] for f in xref_code_columns if row[f] != "")
            description = row["TTY"]

            if lang == "ENG":
                if cui not in cui_to_names:
                    cui_to_names[cui] = {}
                if name not in cui_to_names[cui]:
                    cui_to_names[cui][name] = set()

                cui_to_names[cui][name].add(description)

                if xref_db not in xrefs:
                    xrefs[xref_db] = {}
                if cui not in xrefs[xref_db]:
                    xrefs[xref_db][cui] = set()

                xrefs[xref_db][cui].update(xref_codes)

    return cui_to_names, xrefs


class UmlsKbConfig(BaseKbConfig):
    """
    Default config for NCBI Gene
    """

    resource = Kbs.UMLS
    history = True
    foreign_identifier = False
    string_identifier = True
    local = True
    citations = False


class UmlsKbParser(BaseKbParser):
    """
    Interface to Umls KB
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.xrefs = {}
        self.description_codes[UMLS_SYMBOL] = SYMBOL_CODE

    def get_tradename_relations(self, directory: str) -> dict:
        """
        Load relations of type `tradename` among CUIs
        """

        relations: dict = {}

        relation_types = ["has_tradename", "tradename_of"]

        with open(os.path.join(directory, "MRREL.RRF")) as infile:
            for line in infile:
                if any(r in line for r in relation_types):
                    elems = line.split("|")
                    head, tail = elems[0], elems[4]
                    relations[head] = tail

        return relations

    def populate_description_codes(self, description: Optional[str] = None):
        assert (
            description is not None
        ), "Incremental population: `description` cannot be `None`!"

        # if description not in UMLS_SYMBOLS:
        if description not in self.description_codes:
            self.description_codes[description] = len(self.description_codes)

    def parse_entries(
        self, directory: str, cores: Optional[int] = None
    ) -> Iterator[Entry]:
        """
        Parse file w/ KB entries
        """
        path = os.path.join(directory, "MRCONSO.RRF")

        cui_to_names, xrefs = parse_file(path)

        # TODO: add XREFS table
        # self.xrefs = xrefs

        # NOTE: Add relationships like here?
        # https://github.com/cambridgeltl/sapbert/blob/main/training_data/generate_pretraining_data.ipynb
        # This introduces `identifier duplicates` (different id, same synset).
        # logger.info("Adding drug tradenames...")
        # tradename_relations = self.get_tradename_relations(directory)
        # for head, tail in tradename_relations.items():
        #     if tail in cui_to_names:
        #         cui_to_names[head] = cui_to_names[tail]

        uid = 0
        int_id = 0

        for cui in sorted(cui_to_names):
            names = cui_to_names[cui]

            self.identifier_mapping[cui] = int_id

            # no PN
            if all(d != UMLS_SYMBOL for n, ds in names.items() for d in ds):
                shortest = min(names)
                names[shortest].add(UMLS_SYMBOL)

            for name, descriptions in names.items():
                for d in descriptions:
                    self.populate_description_codes(description=d)

                description = min(self.description_codes[d] for d in descriptions)

                entry = Entry(
                    uid=uid, identifier=int_id, name=name, description=description
                )

                yield entry
                uid += 1

            int_id += 1

    def parse_history_entries(self, directory: str, cores: Optional[int] = None):
        """
        Parse MRCUI.RFF
        See:
            - https://www.ncbi.nlm.nih.gov/books/NBK9684/#ch02.sec2.6
            - https://www.ncbi.nlm.nih.gov/books/NBK9685/table/ch03.T.retired_cui_mapping_file_mrcui_rr/
            - https://www.ncbi.nlm.nih.gov/books/NBK9685/#ch03.sec3.1 (3.3.19.6. Retired CUI Mapping (File = MRCUI.RRF))

        The file includes mappings to current CUIs as synonymous or to one or more related current CUI where possible.
        If a synonymous mapping cannot be found, other relationships between the CUIs can be created.
        These relationships can be Broader (RB), Narrower (RN), Other Related (RO), Deleted (DEL) or Removed from Subset (SUBX).
        """

        path = os.path.join(directory, "MRCUI.RRF")

        with open(path) as infile:
            for line in infile:
                values = line.strip("\n").split("|")
                row = dict(zip(MRCUI_COLUMNS, values))

                if row["REL"] == "SY":
                    entry = HistoryEntry(
                        old_identifier=row["CUI1"], new_identifier=row["CUI2"]
                    )
                elif row["REL"] == "DEL":
                    entry = HistoryEntry(
                        old_identifier=row["CUI1"], new_identifier=OBSOLETE_IDENTIFIER
                    )
                else:
                    continue

                yield entry


def main(args: Namespace):
    """
    Standalone
    """

    schema = BelbKbSchema(
        db_config=args.db, kb_config=UmlsKbConfig(data_dir=args.data_dir)
    )

    converter = KbConverter(directory=args.dir, schema=schema, parser=UmlsKbParser())

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
