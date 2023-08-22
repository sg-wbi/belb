#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Compute basic descriptive statistics for corpora and KB
"""
import argparse
import itertools
import os
from typing import Optional

import pandas as pd
from Levenshtein import ratio
from loguru import logger
from sqlalchemy import and_, select

from belb import (SYMBOL_CODE, AutoBelbCorpus, AutoBelbKb, BelbCorpus, BelbKb,
                  Entities, Queries, Tables)
from belb.kbs.ncbi_gene import NCBI_GENE_SUBSETS
from belb.resources import Corpora, Kbs
from belb.utils import chunkize


def parse_args():
    """
    CLI args
    """

    parser = argparse.ArgumentParser(
        description="Compute basic descriptive statistics for corpora and KB"
    )
    parser.add_argument(
        "--dir", required=True, type=str, help="Directory where BELB data is stored"
    )
    parser.add_argument(
        "--db",
        required=True,
        type=str,
        help="Database configuration",
    )
    parser.add_argument("--out", required=True, type=str, help="Where to store output")

    return parser.parse_args()


def get_qaqc(corpus: BelbCorpus) -> dict:
    qaqc: dict = {}
    qaqc["replaced"] = {}
    qaqc["obsolete"] = {}
    # qaqc["offsets"] = {}
    qaqc["total"] = {}

    for sk, sv in [("test", ["test"])]:
        # for sk, sv in [("train/dev", ["train", "dev"]), ("test", ["test"])]:

        # qaqc["invalid"][sk] = sum(
        #     corpus.qaqc_stats[s].get(k, 0)
        #     for s in sv
        #     for k in ["identifiers_na_pre_kb"]
        # )

        qaqc["replaced"][sk] = sum(
            corpus.qaqc_stats[s].get(k, 0)
            for s in sv
            for k in ["ann_identifier_replaced"]
        )

        qaqc["obsolete"][sk] = sum(
            corpus.qaqc_stats[s].get(k, 0)
            for s in sv
            for k in [
                "ann_identifier_replaced_manual_mapping",
                "ann_identifier_discontinued",
            ]
        )

        qaqc["total"][sk] = qaqc["replaced"][sk] + qaqc["obsolete"][sk]

    return qaqc


def get_zeroshot_and_stratified(annotations: dict) -> tuple[int, int]:
    traindev = annotations["train"] + annotations["dev"]
    traindev_identifiers = set(i for a in traindev for i in a.identifiers)
    traindev_text = set(a.text.lower() for a in traindev)
    test = annotations["test"]

    zeroshot = 0
    stratified = 0
    for a in test:
        if all(i not in traindev_identifiers for i in a.identifiers):
            zeroshot += 1
        elif (
            any(i in traindev_identifiers for i in a.identifiers)
            and a.text.lower() not in traindev_text
        ):
            stratified += 1

    return zeroshot, stratified


def compute_corpora_stats(directory: str) -> pd.DataFrame:
    """
    Compute basic descriptive statistics for BELB corpora
    """

    logger.info("Collecting corpora statistics")

    corpora = [
        (Corpora.GNORMPLUS.name, Entities.GENE),
        (Corpora.NLM_GENE.name, Entities.GENE),
        (Corpora.NCBI_DISEASE.name, Entities.DISEASE),
        (Corpora.BC5CDR.name, Entities.DISEASE),
        (Corpora.BC5CDR.name, Entities.CHEMICAL),
        (Corpora.NLM_CHEM.name, Entities.CHEMICAL),
        (Corpora.LINNAEUS.name, Entities.SPECIES),
        (Corpora.S800.name, Entities.SPECIES),
        (Corpora.BIOID.name, Entities.CELL_LINE),
        (Corpora.SNP.name, Entities.VARIANT),
        (Corpora.OSIRIS.name, Entities.VARIANT),
        (Corpora.TMVAR.name, Entities.VARIANT),
        (Corpora.MEDMENTIONS.name, Entities.UMLS),
    ]

    splits = ["train", "dev", "test"]
    corpora_rows = []
    qaqc_rows = []

    for name, entity_type in corpora:
        corpus_row = {}
        qaqc_row = {}
        corpus_row["name"] = f"{name}_{entity_type}"
        qaqc_row["name"] = f"{name}_{entity_type}"

        corpus = AutoBelbCorpus.from_name(
            name=name,
            directory=directory,
            entity_type=entity_type,
            add_foreign_annotations=False,
        )

        corpus_row["examples"] = " / ".join(
            [
                str(len(corpus.data.get(s))) if corpus.data.get(s) is not None else "-"
                for s in splits
            ]
        )

        annotations = {
            s: [
                a
                for sublist in [
                    [a for a in p.annotations if not a.foreign]
                    for e in examples
                    for p in e.passages
                ]
                for a in sublist
            ]
            for s, examples in corpus.data.items()
        }

        corpus_row["annotations"] = " / ".join(
            [
                str(len(annotations[s])) if annotations.get(s) is not None else "-"
                for s in splits
            ]
        )

        if "train" in annotations:
            zeroshot, stratified = get_zeroshot_and_stratified(annotations=annotations)
            corpus_row[
                "zeroshot"
            ] = f"{zeroshot} ({round(zeroshot / len(annotations['test']) * 100, 2)}\%)"
            corpus_row[
                "stratified"
            ] = f"{stratified} ({round(stratified / len(annotations['test']) * 100, 2)}\%)"
        else:
            corpus_row["zeroshot"] = "-"
            corpus_row["stratified"] = "-"

        corpora_rows.append(corpus_row)

        qaqc = get_qaqc(corpus)
        for k, by_split in qaqc.items():
            qaqc_row[k] = " / ".join(
                [str(v) if v > 0 else "-" for s, v in by_split.items()]
            )

        qaqc_row["total"] = (
            str(qaqc["total"]["test"])
            + " ("
            + str(round(qaqc["total"]["test"] / len(annotations["test"]) * 100, 2))
            + "\%)"
        )

        qaqc_rows.append(qaqc_row)

    corpora_df = pd.DataFrame(corpora_rows)
    qaqc_df = pd.DataFrame(qaqc_rows)

    return corpora_df, qaqc_df


def compute_surface_form_similarity(synset: list):
    """
    Get rough estimate of surface similarity for a given identifier.
    This computes the average similarity of combinations of name in synset
    """

    pairs = list(itertools.combinations(synset, 2))

    # no pairs if there's only one name
    score = 1
    if len(pairs) > 0:
        score = sum(ratio(a, b) for a, b in pairs) / len(pairs)

    return score


def get_base_stats(kb: BelbKb, subset: Optional[list] = None) -> dict:
    """
    Compute base stats: identifiers, names, avg. num names, surface similarity
    """

    logger.debug("{}: sysnset", kb.kb_config.name)

    identifiers = 0
    names = 0
    sfs = 0

    query = kb.queries.get(Queries.SYNSET, subset=subset)

    for row in kb.query(query):
        parsed_row = kb.queries.parse_result(name=Queries.SYNSET, row=row)
        assert isinstance(parsed_row, dict)

        # # exclude synsets with only one name
        # if len(parsed_row["names"]) == 1:
        #     continue

        sfs += compute_surface_form_similarity(synset=parsed_row["names"])

        identifiers += 1
        names += len(parsed_row["names"])

    return {
        "identifiers": identifiers,
        "names": names,
        "avg. names": round(names / identifiers, 2),
        "sfs": round(sfs / identifiers, 2),
    }


def get_homonym_stats(kb: BelbKb, subset: Optional[list] = None) -> dict:
    """
    Collect counts of homonyms
    """

    logger.debug("{}: homonyms", kb.kb_config.name)

    homonyms = set()

    nh = kb.schema.get(Tables.NAME_HOMONYMS)
    if subset is not None:
        query = select(nh).where(nh.c.foreign_identifier.in_(subset))
    else:
        query = select(nh)

    name_homonyms = set(r["uid"] for r in kb.query(query))

    homonyms.update(name_homonyms)

    foreign_homonyms: set = set()
    if kb.kb_config.foreign_identifier:
        fnh = kb.schema.get(Tables.FOREIGN_NAME_HOMONYMS)

        if subset is not None:
            query = select(fnh).where(fnh.c.identifier.in_(subset))
        else:
            query = select(fnh)

        foreign_homonyms.update(set(r["uid"] for r in kb.query(query)))

        homonyms.update(foreign_homonyms)

    table = kb.schema.get(Tables.KB)

    primary_name_homonyms = set()
    for uids in chunkize(homonyms, 10000):
        query = select(table.c.uid).where(
            and_(table.c.uid.in_(uids), table.c.description == SYMBOL_CODE)
        )
        for r in kb.query(query):
            primary_name_homonyms.add(r["uid"])

    return {
        "homonyms": len(homonyms),
        "primary name homonyms": len(primary_name_homonyms),
        "foreign homonyms": len(foreign_homonyms),
        "name homonyms": len(name_homonyms),
    }


def compute_kb_stats(directory: str, db_config: str) -> pd.DataFrame:
    """
    KB stats
    """

    specs = [
        # {"name": Kbs.CTD_DISEASES.name},
        # {"name": Kbs.CTD_CHEMICALS.name},
        # {"name": Kbs.NCBI_TAXONOMY.name},
        # {"name": Kbs.CELLOSAURUS.name},
        # {"name": Kbs.UMLS.name},
        # {"name": Kbs.NCBI_GENE.name, "subset": "gnormplus"},
        # {"name": Kbs.NCBI_GENE.name, "subset": "nlm_gene"},
        # {"name": Kbs.NCBI_GENE.name},
        {"name": Kbs.DBSNP.name},
    ]

    data: list = []

    for spec in specs:
        name = spec["name"]
        subset_name = spec.get("subset")

        if not os.path.exists(os.path.join(directory, "processed", "kbs", name)):
            continue

        logger.info("Collecting statistics for KB: {}", name)

        kb = AutoBelbKb.from_name(
            name=name, directory=directory, db_config=db_config, subset=subset_name
        )

        subset = None
        if spec["name"] == Kbs.NCBI_GENE.name and subset_name is not None:
            subset = NCBI_GENE_SUBSETS[subset_name]

        with kb as handle:
            row: dict = {"kb": name, "subset": subset_name}

            row.update(get_base_stats(handle, subset))

            # row.update(get_homonym_stats(handle, subset))

            print(row)

            data.append(row)

    return pd.DataFrame(data)


def main():
    """
    Run
    """

    args = parse_args()

    os.makedirs(args.out, exist_ok=True)

    logger.info("Collecting BELB statistics")

    # corpora_df, qaqc_df = compute_corpora_stats(args.dir)
    # corpora_df.to_csv(
    #     os.path.join(args.out, "corpora_stats.tsv"), sep="\t", index=False, header=True
    # )
    # qaqc_df.to_csv(
    #     os.path.join(args.out, "qaqc_stats.tsv"), sep="\t", index=False, header=True
    # )

    kb_df = compute_kb_stats(directory=args.dir, db_config=args.db)
    kb_df.to_csv(
        os.path.join(args.out, "dbsnp_stats.tsv"), sep="\t", index=False, header=True
    )


if __name__ == "__main__":
    main()
