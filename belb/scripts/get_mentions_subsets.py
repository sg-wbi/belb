#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Generate list zero-shot and stratified mentions
(see https://aclanthology.org/2020.coling-main.588/)
and homonym mentions.
"""
import argparse
import os

import pandas as pd
from loguru import logger

from belb import AutoBelbCorpus, Entities
from belb.resources import Corpora


def parse_args():
    """
    CLI args
    """

    parser = argparse.ArgumentParser(
        description="Create list of zero-shot and stratified mentions"
    )
    parser.add_argument(
        "--dir", required=True, type=str, help="Directory where BELB data is stored"
    )
    parser.add_argument("--out", required=True, type=str, help="Where to store output")

    return parser.parse_args()


def get_mentions_subsets(directory: str) -> pd.DataFrame:
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

    zeroshot = []
    stratified = []
    # homonyms = []

    for name, entity_type in corpora:
        corpus = AutoBelbCorpus.from_name(
            name=name,
            directory=directory,
            entity_type=entity_type,
            add_foreign_annotations=False,
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

        if "train" in annotations or "dev" in annotations:
            traindev = annotations["train"] + annotations["dev"]
            traindev_identifiers = set(i for a in traindev for i in a.identifiers)
            traindev_text = set(a.text.lower() for a in traindev)
            test = annotations["test"]

            for a in test:
                if all(i not in traindev_identifiers for i in a.identifiers):
                    zeroshot.append(
                        {
                            "corpus": name,
                            "entity_type": a.entity_type,
                            "hexdigest": a.infons["hexdigest"],
                        }
                    )

                elif (
                    any(i in traindev_identifiers for i in a.identifiers)
                    and a.text.lower() not in traindev_text
                ):
                    stratified.append(
                        {
                            "corpus": name,
                            "entity_type": a.entity_type,
                            "hexdigest": a.infons["hexdigest"],
                        }
                    )

    out = {
        "zeroshot": zeroshot,
        "stratified": stratified,
        # "homonyms": homonyms
    }

    return out


def main():
    """Script"""

    args = parse_args()

    subsets = get_mentions_subsets(directory=args.dir)

    pd.DataFrame(subsets["zeroshot"]).to_csv(
        os.path.join(args.out, "zeroshot.csv"), index=False, sep="\t"
    )

    pd.DataFrame(subsets["stratified"]).to_csv(
        os.path.join(args.out, "stratified.csv"), index=False, sep="\t"
    )

    # pd.DataFrame(subsets["homonyms"]).to_csv(
    #     os.path.join(args.out, "homonyms.csv"), index=False, sep="\t"
    # )


if __name__ == "__main__":
    main()
