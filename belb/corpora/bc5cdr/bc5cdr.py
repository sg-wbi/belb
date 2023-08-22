#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Interface for BC5CDR corpus
"""
import os
from argparse import Namespace

from bioc import pubtator

from belb.corpora.corpus import CorpusConverter
from belb.corpora.parser import (BaseBelbCorpusConfig, BaseBelbCorpusParser,
                                 Splits)
from belb.kbs import (BelbKb, BelbKbSchema, CtdChemicalsKbConfig,
                      CtdDiseasesKbConfig)
from belb.preprocessing.data import NA, Annotation, Entities, Example
from belb.resources import Corpora

INVALID_IDENTIFIERS = ["-1"]


class Bc5CdrCorpusConfig(BaseBelbCorpusConfig):
    """
    BC5CDR default configuration
    """

    resource = Corpora.BC5CDR
    splits = [Splits.TRAIN, Splits.DEV, Splits.TEST]
    entity_type = Entities.DISEASE
    entity_types = [Entities.DISEASE, Entities.CHEMICAL]
    pmc = False
    local = False
    title_abstract = True
    entity_type_map = {"Disease": Entities.DISEASE, "Chemical": Entities.CHEMICAL}


class Bc5CdrCorpusParser(BaseBelbCorpusParser):
    """Interface BC5CDR corpus"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.split_to_file = {
            Splits.TRAIN: "CDR_TrainingSet.PubTator.txt",
            Splits.DEV: "CDR_DevelopmentSet.PubTator.txt",
            Splits.TEST: "CDR_TestSet.PubTator.txt",
        }

    def parse_annotation_identifiers(self, original_identifiers: str):
        """Expand identifiers to list"""

        ids_list = original_identifiers.strip().split("|")
        annotation_ids = []
        for i in ids_list:
            # 2234245 534     548     dyschromatopsy  Disease -1
            if i in INVALID_IDENTIFIERS:
                i = NA
            else:
                i = f"MESH:{i}"
            annotation_ids.append(i)
        return annotation_ids

    def load_example(self, document: pubtator.PubTator) -> Example:
        """
        Load example
        """

        assert (
            self.config.entity_type_map is not None
        ), "Corpus with multiple entity types must define `config.entity_type_map`"

        annotations = [Annotation.from_pubtator(a) for a in document.annotations]

        text = {f: getattr(document, f, None) for f in ["title", "abstract"]}

        example = Example.from_text_and_annotations(
            eid=document.pmid, text=text, annotations=annotations
        )

        for p in example.passages:
            annotations = []
            for a in p.annotations:
                if (
                    self.config.entity_type_map[a.entity_type]
                    == self.config.entity_type
                ):
                    annotations.append(a)
            p.annotations = annotations

        return example

    def load_split(self, directory: str, split: str) -> list[Example]:
        """Load examples"""

        path = os.path.join(
            directory,
            "CDR_Data",
            "CDR.Corpus.v010516",
            self.split_to_file[split],
        )

        examples = []

        with open(path) as infile:

            for document in pubtator.iterparse(infile):

                example = self.load_example(document)

                examples.append(example)

        return examples


def main(args: Namespace):
    """
    Standalone
    """

    options = CorpusConverter.extract_config_options_from_args(args)

    for entity_type in Bc5CdrCorpusConfig.entity_types:

        schema = BelbKbSchema(
            db_config=args.db,
            kb_config=CtdDiseasesKbConfig()
            if entity_type == Entities.DISEASE
            else CtdChemicalsKbConfig(),
        )
        kb = BelbKb(directory=args.dir, schema=schema)

        config = Bc5CdrCorpusConfig(entity_type=entity_type, **options)
        parser = Bc5CdrCorpusParser(config=config)
        converter = CorpusConverter(
            directory=args.dir, parser=parser, kb=kb, pubtator=args.pubtator
        )
        converter.to_belb()
