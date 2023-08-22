#!/usr/bin/env pyt/on3
# -*- coding: utf-8 -*-
"""
Interface to Thomas et al. 2011 corpus
Inspired by: https://github.com/bigscience-workshop/biomedical/blob/master/biodatasets/thomas2011/thomas2011.py
"""

import os
from argparse import Namespace

import pandas as pd
from loguru import logger

from belb.corpora.corpus import CorpusConverter
from belb.corpora.parser import (BaseBelbCorpusConfig, BaseBelbCorpusParser,
                                 Splits, qaqcmethod)
from belb.kbs import ENTITY_TO_KB_NAME, AutoBelbKb
from belb.preprocessing.data import (INVALID_POSITION, Annotation, Entities,
                                     Example, Passage)
from belb.resources import Corpora
from belb.utils import load_json, save_json
from belb.utils.pubtator_central import PubTatorAPI

SHIFT_FORWARD_BY_ONE = [
    15645182,
    16368448,
    16453988,
    16497333,
    16652158,
    16691626,
    16723442,
    17019603,
    17216208,
    17219016,
    17299513,
    17445871,
    17535992,
    17566096,
    17630229,
    17632285,
    17656372,
    17657167,
    17674045,
    17701054,
    17701750,
    17704904,
    17852831,
    17873324,
    17877509,
    17894153,
    17917281,
    18061132,
    18092344,
    18160840,
    18162085,
    18163425,
    18193244,
    18222353,
    18239646,
    16865358,
]


SHIFT_FORWARD_BY_THREE = [16351803, 16854283]


class SnpCorpusConfig(BaseBelbCorpusConfig):
    """
    tmVar default configuration
    """

    resource = Corpora.SNP
    splits = [Splits.TEST]
    entity_type = Entities.VARIANT
    entity_types = [Entities.VARIANT]
    pmc = False
    foreign_entity_types = [Entities.GENE]
    add_foreign_annotations = True
    local = False
    title_abstract = True


class SnpCorpusParser(BaseBelbCorpusParser):
    """Interface SNP corpus"""

    @qaqcmethod
    def handle_errors_annotation_offsets(self, eid: str, a: Annotation, p: Passage):
        """Fix annotation offsets"""

        if int(eid) in SHIFT_FORWARD_BY_ONE:
            a.start += 1
            a.end += 1

        elif int(eid) in SHIFT_FORWARD_BY_THREE:
            a.start += 3
            a.end += 3

        elif eid == "17894849" and (a.start, a.end) == (5, 10):
            a.start += 1
            a.end += 1

        elif eid == "17455201" and (a.start, a.end) == (130, 134):
            a.start += 1
            a.end += 1

        elif eid == "17187763" and (a.start, a.end) in [(31, 40), (45, 54)]:
            a.start += 1
            a.end += 1

        elif eid == "16865697" and (a.start, a.end) == (67, 78):
            a.start += 1
            a.end += 1

        elif eid == "17000021" and (a.start, a.end) == (26, 35):
            a.start += 1
            a.end += 1

        elif eid == "17022693" and (a.start, a.end) in [(68, 73), (79, 84)]:
            a.start += 1
            a.end += 1

        elif eid == "16338218" and (a.start, a.end) == (18, 25):
            a.start += 1
            a.end += 1

        elif eid == "16625213" and (a.start, a.end) == (24, 29):
            a.start += 1
            a.end += 1

        elif eid == "17187763" and (a.start, a.end) == (24, 29):
            a.start += 1
            a.end += 1

        elif eid == "17390150" and (a.start, a.end) == (24, 33):
            a.start += 1
            a.end += 1

    def parse_annotation_identifiers(self, original_identifiers: str) -> list[str]:
        """Expand identifiers to list, apply mapping"""

        identifiers = original_identifiers.replace("rs", "")

        return identifiers.split()

    def get_pmid_to_annotations(self, path: str) -> dict:
        """
        Load corpus data
        """

        annotations = pd.read_csv(
            path,
            sep="\t",
            names=["pmid", "text", "hgvs", "start", "end", "dbsnp_id", "type"],
        )

        annotations["pmid"] = annotations["pmid"].astype(str)

        pmid_to_annotations: dict = {}

        for _, row in annotations.iterrows():
            row = row.to_dict()

            pmid = row["pmid"]

            if pmid not in pmid_to_annotations:
                pmid_to_annotations[pmid] = []

            a = Annotation(
                start=row["start"] - 1,
                end=row["end"] - 1,
                text=row["text"].replace("'", ""),
                entity_type=row["type"],
                identifiers=row["dbsnp_id"],
            )

            # offsets are completely wrong
            if pmid == "17094261" and (a.start, a.end) == (1333, 1341):
                a.start = INVALID_POSITION
                a.end = INVALID_POSITION

            if pmid == "17388729" and (a.start, a.end) == (1545, 1550):
                a.start = INVALID_POSITION
                a.end = INVALID_POSITION

            pmid_to_annotations[pmid].append(a)

        return pmid_to_annotations

    def get_pmid_to_text(self, directory: str, pmids: set[str]) -> dict:
        """
        Fetch PMIDs text
        """

        path = os.path.join(directory, "pmid2text.json")

        if not os.path.exists(path):
            pdf = PubTatorAPI(
                download_history_file=os.path.join(directory, "cache.txt")
            )

            pmid_to_text = {}

            for d in pdf.fetch(pmids):
                pmid_to_text[d.id] = {p.infons.get("type"): p.text for p in d.passages}

            save_json(path=path, item=pmid_to_text, kwargs={"json": {"indent": 1}})

        else:
            pmid_to_text = load_json(path)

        return pmid_to_text

    def load_split(self, directory: str, split: str) -> list[Example]:
        """
        Load test examples
        """

        pmid_to_annotations = self.get_pmid_to_annotations(
            os.path.join(directory, "annotations.txt")
        )

        ids = set(pmid_to_annotations.keys())

        examples = []

        pmid_to_text = self.get_pmid_to_text(directory, pmids=ids)

        for pmid, annotations in pmid_to_annotations.items():
            text = pmid_to_text.get(pmid)

            if text is not None:
                example = Example.from_text_and_annotations(
                    eid=pmid, text=text, annotations=annotations
                )

                examples.append(example)

            else:
                logger.warning("PMID: {} not found in PubTator!", pmid)

        return examples


def main(args: Namespace):
    """
    Standalone
    """

    options = CorpusConverter.extract_config_options_from_args(args=args)

    config = SnpCorpusConfig(**options)

    kb = AutoBelbKb.from_name(
        name=ENTITY_TO_KB_NAME[config.entity_type],
        directory=args.dir,
        db_config=args.db,
        debug=args.debug,
    )

    parser = SnpCorpusParser(config=config)
    converter = CorpusConverter(
        directory=args.dir, parser=parser, kb=kb, pubtator=args.pubtator
    )

    converter.to_belb()
