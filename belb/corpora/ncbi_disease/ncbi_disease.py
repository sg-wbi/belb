#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Interface for NCBI Disease corpus
"""

import os
from argparse import Namespace

from bioc import pubtator

from belb.corpora.corpus import CorpusConverter
from belb.corpora.parser import (BaseBelbCorpusParser, BaseBelbCorpusConfig,
                                 Splits, qaqcmethod)
from belb.kbs import BelbKb, BelbKbSchema, CtdDiseasesKbConfig
from belb.preprocessing.data import Annotation, Entities, Example, Passage
from belb.resources import Corpora


class NcbiDiseaseCorpusConfig(BaseBelbCorpusConfig):
    """
    Base config for NCBI Disease corpus
    """

    resource = Corpora.NCBI_DISEASE
    splits = [Splits.TRAIN, Splits.DEV, Splits.TEST]
    entity_type = Entities.DISEASE
    entity_types = [Entities.DISEASE]
    pmc = False
    local = False
    title_abstract = True


class NcbiDiseaseCorpusParser(BaseBelbCorpusParser):
    """Interface NCBI Disease corpus"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.split_to_file = {
            Splits.TRAIN: "NCBItrainset_corpus.txt",
            Splits.DEV: "NCBIdevelopset_corpus.txt",
            Splits.TEST: "NCBItestset_corpus.txt",
        }

    def parse_annotation_identifiers(self, original_identifiers: str) -> list[str]:
        """Expand identifiers to list, apply mapping"""

        ids_list = original_identifiers.strip().split("|")
        ids_list = [i for ids in ids_list for i in ids.split("+")]

        annotation_ids = []
        for i in ids_list:
            if not (i.startswith("OMIM:") or i.startswith("MESH:")):
                i = f"MESH:{i}"
            annotation_ids.append(i)

        return annotation_ids

    @qaqcmethod
    def handle_errors_annotation_text(self, eid: str, a: Annotation, p: Passage):
        """Fix annotation text"""

        if eid == "10802668" and a.text == "autosomal dominant disorde":
            a.end += 1
            a.text = "autosomal dominant disorder"

        if (
            eid == "2792129"
            and a.text == "absence of the seventh component of complemen"
        ):
            a.end += 1
            a.text = "absence of the seventh component of complement"

        if (
            eid == "10923035"
            and a.text == "generalized epilepsy and febrile seizures   plus  "
        ):
            a.text = 'generalized epilepsy and febrile seizures " plus "'

    def load_split(self, directory: str, split: str) -> list[Example]:
        """
        Load examples from split file
        """

        path = os.path.join(directory, self.split_to_file[split])

        examples = []

        loaded = set()

        with open(path) as infile:

            for document in pubtator.iterparse(infile):

                # remove duplicate document: PMID 8528200 in train split
                if document.pmid in loaded:
                    continue

                annotations = [
                    Annotation.from_pubtator(a) for a in document.annotations
                ]

                text = {f: getattr(document, f, None) for f in ["title", "abstract"]}

                example = Example.from_text_and_annotations(
                    eid=document.pmid, text=text, annotations=annotations
                )

                examples.append(example)

                loaded.add(document.pmid)

        return examples


def main(args: Namespace):
    """
    Standalone
    """

    options = CorpusConverter.extract_config_options_from_args(args)
    schema = BelbKbSchema(db_config=args.db, kb_config=CtdDiseasesKbConfig())
    kb = BelbKb(directory=args.dir, schema=schema)
    config = NcbiDiseaseCorpusConfig(**options)
    parser = NcbiDiseaseCorpusParser(config=config)
    converter = CorpusConverter(
        directory=args.dir, parser=parser, kb=kb, pubtator=args.pubtator
    )
    converter.to_belb()
