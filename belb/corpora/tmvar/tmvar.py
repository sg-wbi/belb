#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Interface for tmVar corpus
"""

import os
from argparse import Namespace

from bioc import pubtator

from belb.corpora.corpus import CorpusConverter
from belb.corpora.parser import (BaseBelbCorpusConfig, BaseBelbCorpusParser,
                                 Splits, qaqcmethod)
from belb.kbs import ENTITY_TO_KB_NAME, AutoBelbKb
from belb.preprocessing.data import NA, Annotation, Entities, Example, Passage
from belb.resources import Corpora

# from loguru import logger


TMVAR_VARIANT_ENTITIES = [
    "DNAMutation",
    "ProteinMutation",
    "SNP",
    "AcidChange",
    "ProteinAllele",
    "DNAAllele",
]
TMVAR_ENTITIES = TMVAR_VARIANT_ENTITIES + ["Gene"]

INVALID_IDENTIFIERS = ["-", "D007333"]


class TmVarCorpusConfig(BaseBelbCorpusConfig):
    """
    Osiris default configuration
    """

    resource = Corpora.TMVAR
    splits = [Splits.TEST]
    entity_type = Entities.VARIANT
    entity_types = [Entities.VARIANT, Entities.GENE, Entities.SPECIES]
    foreign_entity_types = [Entities.GENE]
    native_foreign_annotations = True
    title_abstract = True
    pmc = False
    local = False
    add_foreign_annotations = True
    entity_type_map = (
        {e: Entities.VARIANT for e in TMVAR_VARIANT_ENTITIES}
        | {"Gene": Entities.GENE}
        | {"Species": Entities.SPECIES}
    )

    def __init__(self, **kwargs):

        if kwargs.get("entity_type") == Entities.VARIANT:
            kwargs["foreign_entity_types"] = [Entities.GENE]
            kwargs["native_foreign_annotations"] = True

        elif kwargs.get("entity_type") == Entities.GENE:
            kwargs["foreign_entity_types"] = [Entities.SPECIES]
            kwargs["native_foreign_annotations"] = False

        elif kwargs.get("entity_type") == Entities.SPECIES:
            kwargs["foreign_entity_types"] = None
            kwargs["add_foreign_annotations"] = False
            kwargs["native_foreign_annotations"] = False

        super().__init__(**kwargs)


class TmVarCorpusParser(BaseBelbCorpusParser):
    """Interface tmVar corpus"""

    def parse_annotation_identifiers(self, original_identifiers: str) -> list[str]:
        """Expand identifiers to list, apply mapping"""

        if self.config.entity_type == Entities.VARIANT:
            annotation_ids = [NA]
            ids = [
                i for i in original_identifiers.split(";") if i.lower().startswith("rs")
            ]
            if len(ids) > 0:
                annotation_ids = [ids[0].replace("RS#:", "").replace("rs", "")]

        elif self.config.entity_type in [Entities.GENE, Entities.SPECIES]:
            ids_list = original_identifiers.replace(";", ",").split(",")
            annotation_ids = [NA if i in INVALID_IDENTIFIERS else i for i in ids_list]

        return annotation_ids

    @qaqcmethod
    def handle_errors_annotation_offsets(
        self, eid: str, a: Annotation, p: Passage  # pylint: disable=unused-argument
    ):
        if eid == "21904390" and (a.start, a.end) == (343, 347):
            a.start -= 1
            a.end -= 1

        if eid == "21904390" and (a.start, a.end) == (753, 757):
            a.start -= 2
            a.end -= 2

        if eid == "21904390" and (a.start, a.end) == (1156, 1160):
            a.start -= 3
            a.end -= 3

        if eid == "21904390" and (a.start, a.end) == (1487, 1491):
            a.start -= 4
            a.end -= 4

        if eid == "21904390" and (a.start, a.end) == (1631, 1635):
            a.start -= 4
            a.end -= 4

        if eid == "21904390" and (a.start, a.end) == (1645, 1659):
            a.start -= 5
            a.end -= 4

        if eid == "21904390" and (a.start, a.end) == (1640, 1655):
            a.end -= 1

    def get_annotations_from_document(
        self, document: pubtator.PubTator
    ) -> list[Annotation]:
        """
        Build example annotations
        """

        assert (
            self.config.entity_type_map is not None
        ), "Multi-entity corpus must specify `entity_type_map`"

        annotations = []

        for a in document.annotations:

            kwargs = {
                "start": a.start,
                "end": a.end,
                "text": a.text,
                "entity_type": a.type,
            }

            is_annotation = (
                self.config.entity_type_map.get(a.type) == self.config.entity_type
            )
            is_foreign_annotation = (
                self.config.entity_type_map.get(a.type)
                in self.config.foreign_entity_types
            )

            if is_annotation:
                kwargs["identifiers"] = a.id

            elif is_foreign_annotation and self.config.add_foreign_annotations:
                kwargs["foreign"] = True

            else:
                continue

            a = Annotation(**kwargs)

            annotations.append(a)

        return annotations

    def load_example(self, document: pubtator.PubTator) -> Example:
        """
        Load examples from raw files
        """

        text = {f: getattr(document, f, None) for f in ["title", "abstract"]}

        annotations = self.get_annotations_from_document(document)

        example = Example.from_text_and_annotations(
            eid=document.pmid, text=text, annotations=annotations
        )

        return example

    def load_split(self, directory: str, split: str) -> list[Example]:
        """Load split examples"""

        path = os.path.join(directory, "tmVar3Corpus.txt")

        examples = []

        with open(path) as infile:

            collection = pubtator.load(infile)

            for document in collection:

                example = self.load_example(document)

                examples.append(example)

        return examples


def main(args: Namespace):
    """
    Standalone
    """

    # species_subsets = load_species_subsets()
    options = CorpusConverter.extract_config_options_from_args(args)

    # TODO: use all entities
    # for entity_type in TmVarCorpusConfig.entity_types:
    for entity_type in [Entities.VARIANT]:

        kb = AutoBelbKb.from_name(
            name=ENTITY_TO_KB_NAME[entity_type],
            directory=args.dir,
            db_config=args.db,
            debug=args.debug,
        )

        config = TmVarCorpusConfig(entity_type=entity_type, **options)
        parser = TmVarCorpusParser(config=config)

        converter = CorpusConverter(
            directory=args.dir, parser=parser, kb=kb, pubtator=args.pubtator
        )
        converter.to_belb()
