#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Interface for BC2 corpus
"""
import os
from argparse import Namespace

from bioc import pubtator

from belb.corpora.corpus import CorpusConverter
from belb.corpora.parser import (BaseBelbCorpusParser, BaseBelbCorpusConfig,
                                 Splits, qaqcmethod)
from belb.kbs import BelbKb, BelbKbSchema, NcbiGeneKbConfig
from belb.preprocessing.data import NA, Annotation, Entities, Example, Passage
from belb.resources import Corpora

INVALID_IDENTIFIERS = ["-1"]


class GNormPlusCorpusConfig(BaseBelbCorpusConfig):
    """
    GNormPlus (corpus) default configuration
    """

    resource = Corpora.GNORMPLUS
    splits = [Splits.TRAIN, Splits.DEV, Splits.TEST]
    title_abstract = True
    entity_type = Entities.GENE
    entity_types = [Entities.GENE]
    pmc = False
    local = False
    add_foreign_annotations = True
    foreign_entity_types = [Entities.SPECIES, Entities.CELL_LINE]


class GNormPlusCorpusParser(BaseBelbCorpusParser):
    """Interface GNormPlus corpus"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.train_split = None
        # self.human_genes = set()

    def parse_annotation_identifiers(self, original_identifiers: str) -> list[str]:
        """Expand identifiers to list, apply mapping and handle invalid identifiers"""

        # multiple ids
        ids_list = original_identifiers.split(",")
        # remove taxid annotation
        ids_list = [
            i.split("(")[0] if ("Tax:" in i or "tax:" in i) else i for i in ids_list
        ]

        annotation_ids = [NA if i in INVALID_IDENTIFIERS else i for i in ids_list]

        return annotation_ids

    @qaqcmethod
    def handle_errors_annotation_offsets(
        self, eid: str, a: Annotation, p: Passage  # pylint: disable=unused-argument
    ):
        """handle wrong/missing offsets"""

        if eid == "10880510" and a.text == "2P domain mechano-sensitive K+ channel":
            a.end = a.start + len(a.text)

        if eid in ["7520377", "8248204"]:
            a.start -= 1
            a.end -= 1

        if eid == "7649249" and a.text in [
            "ZNF133",
            "ZNF140",
            "ZNF136",
            "ZNF10",
            "KOX1",
        ]:
            a.start -= 1
            a.end -= 1

    @qaqcmethod
    def handle_errors_annotation_text(
        self, eid: str, a: Annotation, p: Passage  # pylint: disable=unused-argument
    ):
        """handle wrong/missing text"""

        if eid == "14576168" and a.start == 1077:
            a.text = a.text.capitalize()

    # def for_human_subset(self, example: Example) -> Example:
    #     """
    #     Remove annotations with non-human genes identifiers
    #     """

    #     for p in example.passages:
    #         pruned_annotations = []
    #         for a in p.annotations:
    #             if a.identifiers is not None:
    #                 if all(i in self.human_genes for i in a.identifiers):
    #                     pruned_annotations.append(a)
    #         p.annotations = pruned_annotations

    #     return example

    # def on_before_load(self, directory: str, kb: BelbKb):
    #     """
    #     Hook to perform operations before data is loaded
    #     """
    #     if self.config.subset == GNormPlusCorpusSubsets.HUMAN:
    #         path = os.path.join(directory, "human.tsv")
    #         if not os.path.exists(path):
    #             logger.info("Fetching human genes...")
    #             table = kb.schema.get(Tables.KB)
    #             query = select(table.c.identifier).where(
    #                 table.c.foreign_identifier == 9606
    #             )
    #             with kb as handle:
    #                 handle.save_query_result(path=path, query=query)
    #         self.human_genes = set(
    #             pd.read_csv(path, sep="\t")["identifier"].astype(str)
    #         )

    # def on_after_load(self, data: dict, kb: BelbKb):
    #     """
    #     Remove non-human gene annotations
    #     """
    #     if self.config.subset == GNormPlusCorpusSubsets.HUMAN:
    #         for split, examples in data.items():
    #             data[split] = [self.for_human_subset(e) for e in examples]

    def load_example(self, document: pubtator.PubTator) -> Example:
        """
        Load examples from raw files
        """

        # skip FamilyDomain, DomainMotif : NO IDENTIFIER
        annotations = [a for a in document.annotations if a.type == "Gene"]

        annotations = [Annotation.from_pubtator(a) for a in annotations]

        text = {f: getattr(document, f, None) for f in ["title", "abstract"]}

        example = Example.from_text_and_annotations(
            eid=document.pmid, text=text, annotations=annotations
        )

        return example

    def load_split(self, directory: str, split: str) -> list[Example]:

        if split == Splits.TRAIN:
            path = os.path.join(directory, "GNormPlusCorpus", "BC2GNtrain.PubTator.txt")
        elif split == Splits.DEV:
            path = os.path.join(directory, "GNormPlusCorpus", "NLMIAT.PubTator.txt")
        else:
            path = os.path.join(directory, "GNormPlusCorpus", "BC2GNtest.PubTator.txt")

        with open(path) as infile:
            documents = list(pubtator.iterparse(infile))

        examples = [self.load_example(document) for document in documents]

        return examples


def main(args: Namespace):
    """
    Standalone
    """

    schema = BelbKbSchema(db_config=args.db, kb_config=NcbiGeneKbConfig())
    kb = BelbKb(directory=args.dir, schema=schema)

    options = CorpusConverter.extract_config_options_from_args(args=args)

    config = GNormPlusCorpusConfig(**options)
    parser = GNormPlusCorpusParser(config=config)
    converter = CorpusConverter(
        directory=args.dir, parser=parser, pubtator=args.pubtator, kb=kb
    )
    converter.to_belb()
