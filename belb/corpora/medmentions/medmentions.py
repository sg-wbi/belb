#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Interface for MedMentions corpus
"""

import os
from argparse import Namespace

from bioc import pubtator
from smart_open import smart_open

from belb.corpora.corpus import CorpusConverter
from belb.corpora.parser import (BaseBelbCorpusConfig, BaseBelbCorpusParser,
                                 Splits, qaqcmethod)
from belb.kbs import BelbKb, BelbKbSchema, UmlsKbConfig
from belb.preprocessing.data import Annotation, Entities, Example, Passage
from belb.resources import Corpora
from belb.utils import StrEnum


def clean_text(text: str) -> str:
    """
    27928244 : This study evaluated the theory of mind (ToM) in adolescents diagnosed with bipolar disorder \u200e\u200e(BD)
    See here: https://stackoverflow.com/questions/51813129/decoding-u200e-to-string
    """
    return text.replace("\u200e", " ")


class MedMentionsSubsets(StrEnum):
    """
    Corpus subsets
    """

    FULL = "full"
    ST21PV = "st21pv"


class MedMentionsCorpusConfig(BaseBelbCorpusConfig):
    """
    BC5CDR default configuration
    """

    resource = Corpora.MEDMENTIONS
    splits = [Splits.TRAIN, Splits.DEV, Splits.TEST]
    entity_type = Entities.UMLS
    entity_types = [Entities.UMLS]
    title_abstract = True
    pmc = False
    local = False
    subset = MedMentionsSubsets.ST21PV
    subsets = list(MedMentionsSubsets)


class MedMentionsCorpusParser(BaseBelbCorpusParser):
    """Interface BC5CDR corpus"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.split_files = {
            Splits.TRAIN: "corpus_pubtator_pmids_trng.txt",
            Splits.DEV: "corpus_pubtator_pmids_dev.txt",
            Splits.TEST: "corpus_pubtator_pmids_test.txt",
        }
        self.corpus = {}

    @qaqcmethod
    def handle_errors_annotation_text(self, eid: str, a: Annotation, p: Passage):
        """Fix annotation text"""

        if eid == "27928244" and (a.start, a.end) == (480, 499):
            a.start += 1
            a.text = p.text[a.start - p.offset : a.end - p.offset]

        if eid == "27928244" and (a.start, a.end) == (678, 687):
            a.start += 1
            a.text = p.text[a.start - p.offset : a.end - p.offset]

    def parse_annotation_identifiers(self, original_identifiers: str) -> list:
        """Expand identifiers to list"""
        return [i.replace("UMLS:", "") for i in original_identifiers.split(",")]

    def load_example(self, document: pubtator.PubTator) -> Example:
        """
        Load example
        """

        text = {f: getattr(document, f, None) for f in ["title", "abstract"]}
        annotations = [Annotation.from_pubtator(a) for a in document.annotations]

        # 27928244 : This study evaluated the theory of mind (ToM) in adolescents diagnosed with bipolar disorder \u200e\u200e(BD)
        # See here: https://stackoverflow.com/questions/51813129/decoding-u200e-to-string
        # if document.pmid == "27928244":
        #     breakpoint()

        for a in annotations:
            a.text = clean_text(a.text)

        for f, t in text.items():
            if t is not None:
                text[f] = clean_text(t)

        example = Example.from_text_and_annotations(
            eid=document.pmid, text=text, annotations=annotations
        )

        return example

    def load_corpus(self, directory: str):
        """
        Load corpus data
        """

        assert (
            self.config.subset is not None
        ), "Corpus with subsets must specify `config.subset`"

        path = os.path.join(
            directory,
            "MedMentions-master",
            self.config.subset,
            "data",
            "corpus_pubtator.txt.gz",
        )

        with smart_open(path, encoding="utf-8") as infile:
            for document in pubtator.iterparse(infile):
                self.corpus[document.pmid] = document

    def load_splits(self, directory: str):
        """
        Load pmids of each split
        """

        for split, file in self.split_files.items():

            path = os.path.join(directory, "MedMentions-master", "full", "data", file)

            pmids = set()

            with open(path) as infile:
                for line in infile:
                    pmid = line.strip()
                    pmids.add(pmid)

            self.splits[split] = pmids

    def on_before_load(self, directory: str, kb: BelbKb):

        self.load_splits(directory=directory)

        self.load_corpus(directory=directory)

    def load_split(self, directory: str, split: str = "train") -> list[Example]:
        """
        Load examples in split
        """

        return [self.load_example(self.corpus[pmid]) for pmid in self.splits[split]]


def main(args: Namespace):
    """
    Standalone
    """

    # TODO: create also FULL subset

    schema = BelbKbSchema(db_config=args.db, kb_config=UmlsKbConfig())
    kb = BelbKb(directory=args.dir, schema=schema)

    options = CorpusConverter.extract_config_options_from_args(args)

    config = MedMentionsCorpusConfig(**options)
    parser = MedMentionsCorpusParser(config=config)
    converter = CorpusConverter(
        directory=args.dir, parser=parser, kb=kb, pubtator=args.pubtator
    )
    converter.to_belb()
