#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Interface to Linnaeus corpus
"""

import os
import re
from argparse import Namespace

import pandas as pd

from belb.corpora.corpus import CorpusConverter
from belb.corpora.parser import BaseBelbCorpusParser, BaseBelbCorpusConfig, Splits
from belb.kbs import BelbKb, BelbKbSchema, NcbiTaxonomyKbConfig
from belb.preprocessing.data import NA, Annotation, Entities, Example, Passage
from belb.resources import Corpora

INVALID_IDENTIFIERS = ["0"]


class LinnaeusCorpusConfig(BaseBelbCorpusConfig):
    """
    Linnaeus default configuration
    """

    resource = Corpora.LINNAEUS
    splits = [Splits.TRAIN, Splits.DEV, Splits.TEST]
    entity_type = Entities.SPECIES
    entity_types = [Entities.SPECIES]
    pmc = True
    local = False
    title_abstract = True


class LinnaeusCorpusParser(BaseBelbCorpusParser):
    """Interface Linnaeus corpus"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.corpus = []

    def load_eid_to_annotations(self, annotations_file: str) -> dict[str, list[dict]]:
        """
        Load file with annotaitons
        """

        # columns = ['#species id','document','start','end','text','code']

        df = pd.read_csv(annotations_file, sep="\t")

        df.rename({df.columns[0]: "identifier"}, axis=1, inplace=True)

        df["identifier"] = df["identifier"].apply(
            lambda x: x.replace("species:ncbi:", "")
        )

        eid2annotations: dict[str, list[dict]] = {}

        for _, row in df.iterrows():

            annotation = row.to_dict()

            annotation.pop("code")

            eid = annotation.pop("document")

            if eid not in eid2annotations:
                eid2annotations[eid] = []

            eid2annotations[eid].append(annotation)

        return eid2annotations

    def parse_annotation_identifiers(self, original_identifiers: str) -> list[str]:
        """Expand identifiers to list, apply mapping"""

        ids_list = original_identifiers.strip().split()

        annotation_ids = []

        for i in ids_list:
            # species:ncbi:0  pmcA1312363     9263    9284    T.aestivum var Fortal   4
            # species:ncbi:0  pmcA1474674     7819    7845    Synechococcus strain Tx-20
            # species:ncbi:0  pmcA1474674     14179   14198   Synechococcus TX-20     4
            # species:ncbi:0  pmcA1851977     17206   17215   SIVmac239
            # species:ncbi:0  pmcA1891629     8906    8928    Rhipicephalus camicasi
            # species:ncbi:0  pmcA1891629     9288    9298    Haemogogus
            # species:ncbi:0  pmcA2562362     1713    1718    mules
            # species:ncbi:0  pmcA2562362     1841    1846    mules
            if i in INVALID_IDENTIFIERS:
                i = NA

            annotation_ids.append(i)

        return annotation_ids

    def split_text_into_passages(
        self, text: str, annotations: list[Annotation]
    ) -> list[dict]:
        """
        # 1. Create passages: split by new line
        # Splitting by `\n` creates a lot of empty/meaningless passages: GROUP/REMOVE THEM
        """
        passages: list[dict] = []
        start = 0
        for idx, match in enumerate(re.finditer(re.escape("\n"), text)):
            end = match.start()
            passages.append(
                {
                    "id": idx,
                    "text": text[start:end],
                    "offset": start,
                    "annotations": [
                        a for a in annotations if a.start >= start and a.end <= end
                    ],
                }
            )
            start = end

        return passages

    def aggregate_short_passages(self, passages: list[dict]) -> list[dict]:
        """
        Aggregate short passages: keep first passage as title
        """
        buckets = []
        bucket: list = []
        # start from bottom to aggregate section titles to correct passages
        for p in passages[::-1]:
            if len(p["text"]) > 100:
                buckets.append(bucket)
                bucket = []
            bucket.append(p)
        if len(bucket) > 0:
            buckets.append(bucket)

        buckets = [sorted(b, key=lambda x: x["id"]) for b in buckets[::-1]]  # fix order

        title = buckets[0].pop(0)
        title["type"] = "title"
        passages = [title]  # title
        if len(buckets[0]) == 0:
            buckets.pop(0)

        for pid, bucket in enumerate(buckets, start=1):

            if pid == len(buckets):
                if all(p["text"] in ["\n", "", "\n\n"] for p in bucket):
                    break

            passage = {"id": pid, "text": "", "annotations": [], "type": NA}
            if pid == 1:
                passage["type"] = "abstract"

            for idx, chunk in enumerate(bucket):
                if idx == 0:
                    passage["offset"] = chunk["offset"]
                passage["text"] += chunk["text"]
                passage["annotations"] += chunk["annotations"]
            passages.append(passage)

        return passages

    def build_passages(self, text: str, annotations: list[Annotation]) -> list[Passage]:
        """Chunkize text into passages"""

        raw_passages = self.split_text_into_passages(text=text, annotations=annotations)

        raw_passages = self.aggregate_short_passages(raw_passages)

        passages = [Passage(**p) for p in raw_passages]

        bypassage_anns = [a for p in passages for a in p.annotations]

        if len(annotations) != len(bypassage_anns):
            raise RuntimeError(
                f"# of annotations in example ({len(annotations)}) != by passage annotations ({len(bypassage_anns)})"
            )

        return passages

    def load_example(self, eid: str, text: str, annotations: list[dict]) -> Example:
        """
        Load raw example from files
        """

        parsed_annotations = [
            Annotation(
                start=a["start"],
                end=a["end"],
                text=a["text"],
                identifiers=a["identifier"],
                entity_type="species",
            )
            for a in annotations
        ]

        passages = self.build_passages(text=text, annotations=parsed_annotations)

        example = Example(id=eid, passages=passages)

        return example

    def load_corpus(self, texts_dir: str, annotations_file: str):
        """
        Load all examples
        """

        eid2annotations = self.load_eid_to_annotations(
            annotations_file=annotations_file
        )

        for eid, annotations in eid2annotations.items():

            text_path = os.path.join(texts_dir, f"{eid}.txt")

            with open(text_path) as infile:
                text = infile.read()

            pmcid = eid.replace("pmcA", "")

            example = self.load_example(eid=pmcid, text=text, annotations=annotations)

            self.corpus.append(example)

    def on_before_load(self, directory: str, kb: BelbKb):

        texts_dir = os.path.join(directory, "manual-corpus-species-1.1", "txt")

        annotations_file = os.path.join(
            directory, "manual-corpus-species-1.1", "tags.tsv"
        )

        self.load_corpus(texts_dir=texts_dir, annotations_file=annotations_file)

    def load_split(self, directory: str, split: str) -> list[Example]:

        examples = [e for e in self.corpus if e.id in self.splits[split]]

        return examples


def main(args: Namespace):
    """
    Standalone
    """

    schema = BelbKbSchema(db_config=args.db, kb_config=NcbiTaxonomyKbConfig())
    kb = BelbKb(directory=args.dir, schema=schema)
    options = CorpusConverter.extract_config_options_from_args(args)
    config = LinnaeusCorpusConfig(**options)
    parser = LinnaeusCorpusParser(config=config)
    converter = CorpusConverter(
        directory=args.dir, parser=parser, kb=kb, pubtator=args.pubtator
    )
    converter.to_belb()
