#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Build PubTator DB.
Data:
- Text and annotations: https://ftp.ncbi.nlm.nih.gov/pub/lu/PubTatorCentral/bioconcepts2pubtatorcentral.offset.gz
- PMC <-> PMID : https://ftp.ncbi.nlm.nih.gov/pub/pmc/PMC-ids.csv.gz
"""


import argparse
import os

import pandas as pd
from bioc import pubtator
from loguru import logger
from pubtator_central import PubTatorDB, PubTatorDbTables
from smart_open import smart_open


def parse_args():
    """
    Parse arguments
    """
    parser = argparse.ArgumentParser(description="Build Pubtator sqlite database.")
    parser.add_argument(
        "--dir",
        required=True,
        type=str,
        help="Directory where `bioconcepts2pubtatorcentral.offset.gz` and `PMC-ids.csv.gz` are stored",
    )
    parser.add_argument(
        "--db",
        required=True,
        type=str,
        help="Database configuration",
    )
    return parser.parse_args()


def populate_abstracts_and_annotations(
    path: str, db: PubTatorDB, chunksize: int = 10000
):
    """
    Create all table for PubTator DB:
        - articles : PMID, TEXT, TITLE
        - pubt_ENTITY: PMID, TYPE, TEXT, START, END
    """

    logger.info(
        "Populating `abstracts` and `annotations` tables with data from `{}`", path
    )

    abstracts_table = db.schema.get(PubTatorDbTables.ABSTRACTS)
    annotations_table = db.schema.get(PubTatorDbTables.ANNOTATIONS)

    VARIANT_ENTITY_TYPES = {"ProteinMutation", "DNAMutation", "SNP"}

    abstracts_data: list = []
    annotations_data: list = []

    with smart_open(path, "r") as pubtator_file:
        for doc in pubtator.iterparse(pubtator_file):
            abstract_row = {
                "pmid": doc.pmid,
                "title": doc.title,
                "abstract": doc.abstract,
            }

            if len(abstracts_data) < chunksize:
                abstracts_data.append(abstract_row)
            else:
                db.populate_table(
                    table=abstracts_table, df=pd.DataFrame(abstracts_data)
                )
                abstracts_data.clear()
                abstracts_data.append(abstract_row)

            for a in doc.annotations:
                a.type = "variant" if a.type in VARIANT_ENTITY_TYPES else a.type.lower()

                annotation_row = {
                    "pmid": doc.pmid,
                    "type": a.type,
                    "text": a.text,
                    "start": a.start,
                    "end": a.end,
                }

                if len(annotations_data) < chunksize:
                    annotations_data.append(annotation_row)
                else:
                    db.populate_table(
                        table=annotations_table, df=pd.DataFrame(annotations_data)
                    )
                    annotations_data.clear()
                    annotations_data.append(annotation_row)

    # FLUSH REMAINING ROWS
    if len(abstracts_data) > 0:
        db.populate_table(table=abstracts_table, df=pd.DataFrame(abstracts_data))
        abstracts_data.append(abstract_row)

    if len(annotations_data) > 0:
        db.populate_table(table=annotations_table, df=pd.DataFrame(annotations_data))
        annotations_data.append(annotation_row)


def populate_pmcid_to_pmid(path: str, db: PubTatorDB):
    """
    Populate table `pmcid_to_pmid`
    """

    logger.info("Populating `pmcid_to_pmid` table with data from `{}`", path)

    table = db.schema.get(PubTatorDbTables.PMCID_TO_PMID)

    reader = pd.read_csv(path, chunksize=10000)

    for df in reader:
        df = df[["PMCID", "PMID"]].copy(deep=True)
        df.dropna(inplace=True)
        df["PMCID"] = df["PMCID"].apply(lambda x: x.replace("PMC", "")).astype(int)
        df = df.rename(columns={name: name.lower() for name in ("PMCID", "PMID")})
        if not df.to_dict("records"):
            continue
        db.populate_table(table=table, df=df)


def main():
    """
    Run
    """

    args = parse_args()

    logger.info("Start creating PubTator tables...")

    db = PubTatorDB(directory=args.dir, db_config=args.db)

    with db as handle:
        handle.init_database()

        populate_pmcid_to_pmid(path=os.path.join(args.dir, "PMC-ids.csv.gz"), db=handle)

        populate_abstracts_and_annotations(
            path=os.path.join(args.dir, "bioconcepts2pubtatorcentral.offset.gz"),
            db=handle,
        )

    logger.info("Completed creation of PubTator tables")


if __name__ == "__main__":
    main()
