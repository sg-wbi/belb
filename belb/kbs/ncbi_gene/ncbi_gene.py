#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Interfaces to NCBI Gene
"""
import multiprocessing as mp
import os
from argparse import Namespace
from typing import Iterator, Optional

import pandas as pd

from belb.kbs.kb import BelbKb, KbConverter
from belb.kbs.ncbi_taxonomy import NcbiTaxonomyKbConfig
from belb.kbs.parser import BaseKbConfig, BaseKbParser
from belb.kbs.schema import BelbKbSchema
from belb.preprocessing.data import (OBSOLETE_IDENTIFIER, SYMBOL_CODE, Entry,
                                     HistoryEntry)
from belb.resources import Kbs
from belb.utils import load_kb_subsets

NCBI_GENE_NA = "-"
NCBI_GENE_ATTRIBUTES = {
    "map_location": "loc",
    "LocusTag": "locus",
    "chromosome": "chr",
    "type_of_gene": "type",
    # "GeneID": "id:",  # last resort?
}

NCBI_GENE_SYMBOL = "Symbol"
NCBI_GENE_SYNONYMS = tuple(
    [
        "Symbol_from_nomenclature_authority",
        "Full_name_from_nomenclature_authority",
        "description",
        "Synonyms",
        "Other_designations",
    ]
)

EMPTY_ENTRY_TEXT = [
    "when different from all specified ones in Gene.",
    "Record to support submission of GeneRIFs for a gene not in Gene",
]

NCBI_GENE_SUBSETS = load_kb_subsets(Kbs.NCBI_GENE.name)


class NcbiGeneKbConfig(BaseKbConfig):
    """
    Default config for NCBI Gene
    """

    resource = Kbs.NCBI_GENE
    history = True
    foreign_identifier = True
    string_identifier = False
    citations = False
    attribute = True
    local = False
    subsets = NCBI_GENE_SUBSETS
    foreign_kb = Kbs.NCBI_TAXONOMY.name


def is_na(n: str):
    """
    Check if missing
    """

    return n == NCBI_GENE_NA


def is_invalid_name(n: str):
    """
    Not real name
    """

    newentry = n == "NEWENTRY"
    empty = n == ""
    text_comment = any(e in n for e in EMPTY_ENTRY_TEXT)

    return any([newentry, empty, text_comment])


def parse_names(names: str) -> list[str]:
    """
    Extract list of gene names
    """

    names_list = [name.replace("'", "") for name in names.split("|")]
    names_list = [n for n in names_list if not is_invalid_name(n)]
    return names_list


def get_synonyms(row: dict) -> dict:
    """
    Extract gene synonyms
    """

    synonyms: dict = {}

    for field in NCBI_GENE_SYNONYMS:
        names = row.get(field, NCBI_GENE_NA)
        if is_na(names):
            continue

        parsed_names = parse_names(names)

        for name in parsed_names:
            if name not in synonyms:
                synonyms[name] = field

    return synonyms


def get_attribute(row: dict) -> str:
    """
    Get gene `attribute`
    """
    attribute = None
    for field, prefix in NCBI_GENE_ATTRIBUTES.items():
        a = row.get(field, NCBI_GENE_NA)
        if not is_na(a):
            attribute = f"{prefix}:{a}"
            break

    assert attribute is not None, f"Could not find attribute for row: {row}"

    return attribute


def parse_row(row: dict) -> list[dict]:
    """
    Extract entry from row
    """

    symbol = row[NCBI_GENE_SYMBOL]

    entries = []

    if not is_invalid_name(symbol):
        identifier = row["GeneID"]

        species = row["#tax_id"]

        name2desc: dict = {}

        name2desc[symbol] = NCBI_GENE_SYMBOL

        synonyms = get_synonyms(row=row)

        for name, description in synonyms.items():
            if name not in name2desc:
                name2desc[name] = description

        attribute = get_attribute(row=row)
        entries = [
            {
                "identifier": identifier,
                "name": name,
                "foreign_identifier": species,
                "description": desc,
                "attribute": attribute,
            }
            for name, desc in name2desc.items()
        ]

    return entries


def parse_history_row(row: dict) -> HistoryEntry:
    """
    Extract history row
    """

    new_identifier = row["GeneID"]

    entry = HistoryEntry(
        old_identifier=row["Discontinued_GeneID"],
        new_identifier=new_identifier if new_identifier != "-" else OBSOLETE_IDENTIFIER,
    )

    return entry


class NcbiGeneKbParser(BaseKbParser):
    """
    Interface to NCBI Gene KB
    """

    def populate_description_codes(
        self, description: Optional[str] = None  # pylint: disable=unused-argument
    ):
        """
        Define description codes
        """

        self.description_codes.update({NCBI_GENE_SYMBOL: SYMBOL_CODE})

        for field in NCBI_GENE_SYNONYMS:
            self.description_codes[field] = len(self.description_codes)

    def parse_entries(
        self, directory: str, cores: Optional[int] = None
    ) -> Iterator[Entry]:
        """
        Extract entries from KB file
        """

        cores = cores if cores is not None else 2
        batch_size = 10000
        chunksize = batch_size * cores

        file = os.path.join(directory, "gene_info.gz")
        reader = pd.read_csv(
            file,
            chunksize=chunksize,
            sep="\t",
            na_filter=False,
            low_memory=False,
            compression="gzip",
        )

        self.populate_description_codes()

        uid = 0

        with mp.Pool(cores) as pool:
            for chunk in reader:
                rows = [row.to_dict() for rowidx, row in chunk.iterrows()]

                for entries in pool.imap_unordered(parse_row, rows, batch_size):
                    for entry in entries:
                        entry["uid"] = uid
                        entry["description"] = self.description_codes[
                            entry["description"]
                        ]
                        self.foreign_identifiers.add(entry["foreign_identifier"])

                        yield Entry(**entry)

                        uid += 1

    def parse_history_entries(self, directory: str, cores: Optional[int] = None):
        """
        Parse history entries
        """

        cores = cores if cores is not None else 2
        batch_size = 10000
        chunksize = batch_size * cores

        file = os.path.join(directory, "gene_history.gz")

        reader = pd.read_csv(
            file,
            chunksize=chunksize,
            sep="\t",
            na_filter=False,
            compression="gzip",
        )

        with mp.Pool(cores) as pool:
            for chunk in reader:
                rows = [row.to_dict() for rowidx, row in chunk.iterrows()]

                for entry in pool.imap_unordered(parse_history_row, rows, batch_size):
                    yield entry


def main(args: Namespace):
    """
    Standalone
    """
    schema = BelbKbSchema(db_config=args.db, kb_config=NcbiGeneKbConfig())

    converter = KbConverter(
        directory=args.dir, schema=schema, parser=NcbiGeneKbParser()
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
        handle.init_database(chunksize=100000)
        foreign_schema = BelbKbSchema(
            db_config=args.db, kb_config=NcbiTaxonomyKbConfig()
        )
        foreign_kb = BelbKb(directory=args.dir, schema=foreign_schema, debug=args.debug)
        handle.update_foreign_identifiers(foreign_kb=foreign_kb)
