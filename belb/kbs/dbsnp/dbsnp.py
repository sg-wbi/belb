#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Interfaces to dbSNP.

Fetch data from: ftp://ftp.ncbi.nlm.nih.gov/snp/redesign/latest_release/JSON/
"""

import json
import multiprocessing as mp
import os
from argparse import Namespace
from collections import defaultdict
from typing import Iterator, Optional

from loguru import logger
from smart_open import smart_open

# from belb.kbs.ncbi_gene import NcbiGeneKbConfig
from belb.kbs.kb import BelbKb, KbConverter
from belb.kbs.parser import BaseKbConfig, BaseKbParser
from belb.kbs.schema import BelbKbSchema
from belb.preprocessing.data import (
    CHROMOSOMES,
    OBSOLETE_IDENTIFIER,
    SYMBOL_CODE,
    Entry,
    HistoryEntry,
)
from belb.resources import Kbs
from belb.utils import CompressedFileWriter, chunkize

# NOT CLEAR HOW TO USE `refsnp-merged.json.bz2`
# if `merged_into` has more than one value when checking the web version
# Sometimes it's first one
# {
#     "refsnp_id": "1569035306",
#     "merged_snapshot_data": {
#         "proxy_time": "2020-04-27T20:38Z",
#         "proxy_build_id": "154",
#         "merged_into": ["28735595", "202238012"],
#     },
#     "mane_select_ids": [],
# }
# sometimes not
# {
#     "refsnp_id": "1589172216",
#     "merged_snapshot_data": {
#         "proxy_time": "2021-04-26T15:04Z",
#         "proxy_build_id": "155",
#         "merged_into": ["59795796", "1565098448"],
#     },
#     "mane_select_ids": [],
# }


def parse_line(line: str) -> tuple[list[dict], dict]:
    """
    Extract rsid, HGVS notations and related gene from json line in dbSNP dump

    Ported to python from this implementation:

        https://github.com/rockt/SETH/blob/master/src/main/java/de/hu/berlin/wbi/stuff/dbSNPParser/json/ParseJSONToFile.java
    """

    row = json.loads(line)

    rsid = row["refsnp_id"]

    citations = {pmid: row["refsnp_id"] for pmid in row["citations"]}

    assembly_annotations = [
        aa
        for allele_annotations in row["primary_snapshot_data"]["allele_annotations"]
        for aa in allele_annotations["assembly_annotation"]
    ]

    genes = [gene for aa in assembly_annotations for gene in aa["genes"]]
    gene_ids = list(set(gene["id"] for gene in genes))
    # some entries do not report the gene id
    # https://www.ncbi.nlm.nih.gov/snp/?term=171
    gene_ids = [-1] if len(gene_ids) == 0 else gene_ids

    # add HGVS notations from `assembly_annotation`: from SETH
    # refseq_hgvs = set(rna.get("hgvs") for gene in genes for rna in gene.get("rnas", []))

    # add HGVS notations from placements_with_allele: NEW
    # e.g. rs757229 would have no HGVS names
    # but https://www.ncbi.nlm.nih.gov/snp/?term=757229[uid] has!
    refseq_hgvs = set(
        a.get("hgvs")
        for pwa in row["primary_snapshot_data"]["placements_with_allele"]
        for a in pwa["alleles"]
        # make disegual by default since some entries do not have a `spdi` field
        # htps://www.ncbi.nlm.nih.gov/snp/?term=2066730
        # [a for pwa in d['primary_snapshot_data']['placements_with_allele'] for a in pwa['alleles'] if a['allele'].get('spdi') is None ]
        # [{'allele': {'frameshift': {'seq_id': 'NP_077734.2', 'position': 397}},'hgvs': 'NP_077734.2:p.Ser398fs'},
        # {'allele': {'frameshift': {'seq_id': 'NP_001298122.2', 'position': 337}}, 'hgvs': 'NP_001298122.2:p.Ser338fs'}, ...]
        if a["allele"].get("spdi", {}).get("deleted_sequence", "G")
        != a["allele"].get("spdi", {}).get("inserted_sequence", "A")
    )

    refseq_hgvs = set(
        rh for rh in refseq_hgvs if (rh is not None and rh != "not_yet_implemented")
    )

    hgvs = set()
    for rh in refseq_hgvs:
        parts = rh.split(":")
        if len(parts) != 2:
            logger.warning(
                "RSID:{} - failed to split into (RefSeq,HGVS): {}",
                rsid,
                rh,
            )
            continue

        refseq, name = parts

        # See http://www.ncbi.nlm.nih.gov/books/NBK21091/table/ch18.T.refseq_accession_numbers_and_mole/?report=objectonly
        # Skip XM, XR, and XP, which are are automatically derived annotation pipelines
        # NC_ NM_ NG_ NR_ NP_ NT_ NW_
        if refseq.startswith(("XM_", "XR_", "XP_", "GPC_", "YP_")):
            continue

        hgvs.add(name)

    long_names = set(name for name in hgvs if len(name) > 255)

    # reduce size of database by skipping long hgvs names
    # only if they are not all long though!
    if len(long_names) != len(hgvs):
        hgvs = set(name for name in hgvs if name not in long_names)

    entries = []
    for gene_id in gene_ids:
        entries += [
            {
                "identifier": rsid,
                "name": name,
                "foreign_identifier": gene_id,
                "description": 1,
            }
            for name in hgvs
        ]

    return entries, citations


def get_lines(directory: str) -> Iterator[str]:
    """
    Generate dbSNP rows
    """

    for c in CHROMOSOMES:
        file = os.path.join(directory, f"refsnp-chr{c}.json.bz2")

        logger.info("Start parsing file: `{}`", file)

        with smart_open(file, encoding="utf-8") as infile:
            for line in infile:
                yield line


class DbSnpKbConfig(BaseKbConfig):
    """
    Base config for dbSNP KB
    """

    resource = Kbs.DBSNP
    history = True
    foreign_identifier = True
    string_identifier = False
    citations = True
    local = True
    foreign_kb = Kbs.NCBI_GENE.name


class DbSnpKbParser(BaseKbParser, CompressedFileWriter):
    """
    Interface to dbSNP
    """

    def populate_description_codes(self, description: Optional[str] = None):
        self.description_codes.update({"rsid": SYMBOL_CODE, "hgvs": 1})

    def parse_entries(
        self, directory: str, cores: Optional[int] = None
    ) -> Iterator[Entry]:
        """
        Get entries
        """

        self.populate_description_codes()

        cores = cores if cores is not None else 2
        batch_size = 100000
        chunksize = batch_size * cores

        corpus = defaultdict(set)

        uid = 0

        with mp.Pool(cores) as pool:
            for lines in chunkize(get_lines(directory), chunksize=chunksize):
                for entries, citations in pool.imap_unordered(
                    parse_line, lines, batch_size
                ):
                    for pmid, rsid in citations.items():
                        corpus[pmid].add(rsid)

                    for entry in entries:
                        entry["uid"] = uid

                        self.foreign_identifiers.add(
                            entry["foreign_identifier"])

                        yield Entry(**entry)

                        uid += 1

        self.citations.update(
            {k: [int(rsid) for rsid in v]
             for k, v in corpus.items() if len(v) > 0}
        )

    def parse_history_entries(self, directory: str, cores: Optional[int] = None):
        """
        Get history entries
        """

        history = {}

        logger.debug("Parsing merged rsids...")

        merged_path = os.path.join(directory, "refsnp-merged.json.bz2")

        with smart_open(merged_path, encoding="utf-8") as fp:
            for line in fp:
                d = json.loads(line)
                rsid = d["refsnp_id"]
                merged_into = d.get("merged_snapshot_data",
                                    {}).get("merged_into", [])

                if len(merged_into) == 0:
                    new_identifier = OBSOLETE_IDENTIFIER
                elif len(merged_into) == 1:
                    new_identifier = merged_into[0]
                else:
                    new_identifier = ";".join(merged_into)

                if rsid not in history:
                    history[rsid] = new_identifier
                for m in d["dbsnp1_merges"]:
                    if m["merged_rsid"] not in history:
                        history[m["merged_rsid"]] = new_identifier

        logger.debug("Parsing withdrawn rsids...")
        for file in ["refsnp-unsupported.json.bz2", "refsnp-withdrawn.json.bz2"]:
            with smart_open(os.path.join(directory, file), encoding="utf-8") as fp:
                for line in fp:
                    d = json.loads(line)
                    rsid = d["refsnp_id"]
                    if rsid not in history:
                        history[rsid] = OBSOLETE_IDENTIFIER

        for old_identifier, new_identifier in history.items():
            yield HistoryEntry(
                old_identifier=old_identifier, new_identifier=new_identifier
            )


def main(args: Namespace):
    """
    Standalone
    """

    schema = BelbKbSchema(
        db_config=args.db, kb_config=DbSnpKbConfig(data_dir=args.data_dir)
    )

    converter = KbConverter(
        directory=args.dir, schema=schema, parser=DbSnpKbParser())

    converter.to_belb(
        cores=args.cores,
        log_every_n_entries=int(1e7),
        skip_kb=args.skip_kb,
        skip_history=args.skip_history,
        overwrite=args.overwrite,
    )

    kb = BelbKb(directory=args.dir, schema=schema, debug=args.debug)
    with kb as handle:
        handle.init_database(chunksize=100000)
        # foreign_schema = BelbKbSchema(db_config=args.db, kb_config=NcbiGeneKbConfig())
        # foreign_kb = BelbKb(directory=args.dir, schema=foreign_schema, debug=args.debug)
        # handle.update_foreign_identifiers(foreign_kb=foreign_kb)


###################################################
# OLD PARSING
###################################################
# def extract_row_data(json_dict: dict):

#     rsid = row["refsnp_id"]

#     history_entries = [
#         HistoryEntry(old_identifier=merge["merged_rsid"], new_identifier=rsid)
#         for merge in row["dbsnp1_merges"]
#     ]

#     # pylint: disable=too-many-nested-blocks
#     allele_annotations = json_dict["primary_snapshot_data"]["allele_annotations"]
#     for allele_annotation in allele_annotations:
#         assembly_annotations = allele_annotation["assembly_annotation"]
#         for assembly_annotation in assembly_annotations:
#             genes = assembly_annotation["genes"]
#             for gene in genes:
#                 gene_id = gene["id"]
#                 rnas = gene["rnas"]
#                 for rna in rnas:
#                     refseq_hgvs = rna.get("hgvs")
#                     if refseq_hgvs is not None:
#                         elems = refseq_hgvs.split(":")
#                         if len(elems) != 2:
#                             logger.warning(
#                                 "RSID:{} - failed to split into (RefSeq,HGVS): {}",
#                                 rsid,
#                                 refseq_hgvs,
#                             )
#                             continue
#                         refseq, hgvs = elems

#                         # See http://www.ncbi.nlm.nih.gov/books/NBK21091/
#                         # table/ch18.T.refseq_accession_numbers_and_mole/?report=objectonly
#                         # Skip XM, XR, and XP, which are are automatically derived annotation pipelines
#                         # NC_ NM_ NG_ NR_ NP_ NT_ NW_
#                         if refseq.startswith(("XM_", "XR_", "XP_", "GPC_", "YP_")):
#                             continue

#                         if len(hgvs) > 255:
#                             logger.warning("Skip too long HGVS: {}", hgvs)
#                             continue

#                         entries_data.add((rsid, hgvs, gene_id))

#     entries = [
#         {
#             "identifier": ed[0],
#             "name": ed[1],
#             "foreign_identifier": ed[2],
#             "description": 1,
#         }
#         for ed in entries_data
#     ]
#     return entries, history_entries
