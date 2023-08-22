#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Interface for S800 corpus
"""

import os
from argparse import Namespace

import pandas as pd

from belb.corpora.corpus import CorpusConverter
from belb.corpora.parser import BaseBelbCorpusParser, BaseBelbCorpusConfig, Splits
from belb.kbs import BelbKb, BelbKbSchema, NcbiTaxonomyKbConfig
from belb.preprocessing.data import (INVALID_POSITION, NA, Annotation,
                                     Entities, Example, Passage)
from belb.resources import Corpora


class S800CorpusConfig(BaseBelbCorpusConfig):
    """
    S800 default configuration
    """

    name = "s800"
    resource = Corpora.S800
    splits = [Splits.TRAIN, Splits.DEV, Splits.TEST]
    entity_type = Entities.SPECIES
    entity_types = [Entities.SPECIES]
    pmc = False
    local = False
    title_abstract = False


class S800CorpusParser(BaseBelbCorpusParser):
    r"""
    It's called S800 because it should contain 800 documents.
    But the annotation file provides annotation only for 625.
    ¯\_(ツ)_/¯
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.corpus: list[Example] = []

    def load_eid_to_annotations(self, annotations_file: str):
        """
        Load annotations of each example
        """

        columns = ["identifier", "id:pmid", "start", "end", "text"]
        df = pd.read_csv(annotations_file, sep="\t", names=columns)  # type: ignore

        df["id"] = df["id:pmid"].apply(lambda x: x.split(":")[0])
        df["pmid"] = df["id:pmid"].apply(lambda x: x.split(":")[1])
        df.drop(["id:pmid"], axis=1, inplace=True)

        eid2annotations: dict[str, list[dict]] = {}
        eid2pmid: dict[str, str] = {}

        for _, row in df.iterrows():
            annotation = row.to_dict()

            eid = annotation["id"]

            if eid not in eid2pmid:
                eid2pmid[eid] = annotation["pmid"]

            if eid not in eid2annotations:
                eid2annotations[eid] = []

            eid2annotations[eid].append(annotation)

        return eid2annotations, eid2pmid

    def parse_annotation_identifiers(self, original_identifiers: str) -> list[str]:
        """Expand identifiers to list, apply mapping"""

        annotation_ids = str(original_identifiers).strip().split()

        return annotation_ids

    def load_example(self, eid: str, text: str, annotations: list[dict]):
        """
        Load raw example from file

        Parameters
        ----------
        eid : str
            Example identifier
        text : str
            Example text (title,abstract)
        annotations : list[dict]
            Example annotations
        """

        parsed_annotations = [
            Annotation(
                start=a.get("start", INVALID_POSITION),
                end=a.get("end", INVALID_POSITION),
                text=a.get("text", NA),
                identifiers=a.get("identifier", NA),
                entity_type=self.config.entity_type,
            )
            for a in annotations
        ]

        # need to to this to all to get the offsets working
        for a in parsed_annotations:
            a.end += 1

        passage = Passage(
            id=0, offset=0, text=text, annotations=parsed_annotations, type=NA
        )

        example = Example(id=eid, passages=[passage])

        return example

    def load_corpus(self, texts_dir: str, annotations_file: str):
        """
        Load all corpus
        """

        eid2annotations, eid2pmid = self.load_eid_to_annotations(
            annotations_file=annotations_file
        )

        for eid, annotations in eid2annotations.items():

            text_path = os.path.join(texts_dir, f"{eid}.txt")

            with open(text_path) as infile:
                text = infile.read()

            example = self.load_example(
                eid=eid2pmid.get(eid), text=text, annotations=annotations
            )

            self.corpus.append(example)

    def on_before_load(self, directory: str, kb: BelbKb):

        texts_dir = os.path.join(directory, "abstracts")

        annotations_file = os.path.join(directory, "S800.tsv")

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
    config = S800CorpusConfig(**options)
    parser = S800CorpusParser(config=config)
    converter = CorpusConverter(
        directory=args.dir, parser=parser, kb=kb, pubtator=args.pubtator
    )
    converter.to_belb()
