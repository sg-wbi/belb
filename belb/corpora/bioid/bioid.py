#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Interface for BioID corpus

DO NOT USE ANNOTATIONS FOUND IN BIOC FILES!!! USE THOSE PROVIDED IN `annotations.csv`!!!
"""


import copy
import os
from argparse import Namespace

import bioc
import pandas as pd
from loguru import logger

from belb.corpora.corpus import CorpusConverter
from belb.corpora.parser import (BaseBelbCorpusParser, BaseBelbCorpusConfig,
                                 Splits, qaqcmethod)
from belb.kbs import ENTITY_TO_KB_NAME, AutoBelbKb, BelbKb
from belb.preprocessing.data import NA, Annotation, Entities, Example, Passage
from belb.resources import Corpora

# BIOID_NO_NORMALIZATION = [
#     "cell",
#     "gene",
#     "molecule",
#     "protein",
#     "subcellular",
#     "tissue",
# ]
# BIOID_UNSUPPORTED_KBS = [
#     "BAO",
#     "CHEBI",
#     "CL",
#     "Corum",
#     "GO",
#     "PubChem",
#     "Rfam",
#     "Uberon",
# ]


def clean_text(text: str) -> str:
    """
    Replace hyphen and minus w/ dash
    """

    return text.replace("\u2010", "-").replace("\u2212", "-")


class BioIdCorpusConfig(BaseBelbCorpusConfig):
    """
    Bio ID default configuration
    """

    resource = Corpora.BIOID
    splits = [Splits.TRAIN, Splits.DEV, Splits.TEST]
    entity_type = Entities.CELL_LINE
    entity_types = [Entities.CELL_LINE, Entities.GENE, Entities.SPECIES]
    pmc = True
    local = False
    title_abstract = False
    add_foreign_annotations = True
    native_foreign_annotations = True
    foreign_entity_types = [Entities.SPECIES]
    entity_type_map = {
        "NCBI gene": Entities.GENE,
        "Uniprot": Entities.GENE,  # Uniprot -> NCBI Gene mapping provided by authors of corpus
        "Cellosaurus": Entities.CELL_LINE,
        "NCBI taxon": Entities.SPECIES,
        "organism": Entities.SPECIES,
    }

    def __init__(self, **kwargs):

        if kwargs.get("entity_type") == Entities.CELL_LINE:
            kwargs["foreign_entity_types"] = [Entities.SPECIES]

        elif kwargs.get("entity_type") == Entities.GENE:
            kwargs["foreign_entity_types"] = [Entities.SPECIES]

        elif kwargs.get("entity_type") == Entities.SPECIES:
            kwargs["add_foreign_annotations"] = False
            kwargs["native_foreign_annotations"] = False
            kwargs["foreign_entity_types"] = None

        super().__init__(**kwargs)


class BioIdCorpusParser(BaseBelbCorpusParser):
    """Interface BC5CDR corpus"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.normalized_entities = ["NCBI gene", "Uniprot", "Cellosaurus", "NCBI taxon"]
        self.uniprot_to_ncbi_gene = {}
        self.uniprot_no_mapping = set()
        self.eid_to_annotations = {}

    @qaqcmethod
    def handle_errors_annotation_offsets(
        self, eid: str, a: Annotation, p: Passage  # pylint: disable=unused-argument
    ):
        if eid == "3988959" and (a.start, a.end) == (6128, 6139):
            a.end -= 1

        if eid == "3746197" and (a.start, a.end) == (4629, 4636):
            a.end -= 1

    def parse_annotation_identifiers(self, original_identifiers: str) -> list[str]:
        """Expand identifiers to list, apply mapping"""

        if self.config.entity_type in [Entities.GENE, Entities.SPECIES]:
            # 5125832,Figure_3-G,aspartate aminotransferase,Uniprot:P05201|Uniprot:P05202
            db_identifier = [
                tuple(i.split(":")) for i in original_identifiers.split("|")
            ]
        elif self.config.entity_type == Entities.CELL_LINE:
            db_identifier = [
                ("Cellosaurus", i) for i in original_identifiers.split("|")
            ]

        identifiers = []

        for db, identifier in db_identifier:
            if db == "Uniprot":
                if identifier not in self.uniprot_to_ncbi_gene:
                    self.uniprot_no_mapping.add(identifier)
                    identifier = NA
                else:
                    identifier = self.uniprot_to_ncbi_gene[identifier]

            identifiers.append(identifier)
        return identifiers

    def load_eid_to_annotations(self, directory: str):
        """
        Load file with annotaitons
        """

        annotations_file = os.path.join(directory, "BioIDtraining_2", "annotations.csv")

        logger.info("Start loading corpus annotations...")

        df = pd.read_csv(annotations_file, sep=",")

        for _, row in df.iterrows():

            row = row.to_dict()

            eid = str(row["don_article"])

            if eid not in self.eid_to_annotations:
                self.eid_to_annotations[eid] = {}

            figure = row["figure"].replace("_", " ")

            if figure not in self.eid_to_annotations[eid]:
                self.eid_to_annotations[eid][figure] = []

            a = {
                "start": row["first left"],
                "end": row["last right"],
                "text": row["text"],
                "identifiers": row["obj"],
                "location": figure,
            }

            self.eid_to_annotations[eid][figure].append(a)

    def load_uniprot_to_ncbi_gene(self, directory: str):
        """
        Load mapping `Uniprot -> NCBI Gene`
        """

        logger.info("Loading provided mapping from Uniprot to NCBI Gene...")

        path = os.path.join(directory, "BioIDtraining_2", "mapping_training.xlsx")

        df = pd.read_excel(path)

        self.uniprot_to_ncbi_gene = dict(
            zip(list(df["Uniprot"]), list(df["NCBI gene"]))
        )

    def unpack_entity_type_identifier(self, entity_type_identifier: str):
        """
        Extract entity and identifier from raw format, e.g.: Uniprot:P05201
        """

        items = entity_type_identifier.split(":")

        entity_type = None

        # ids from cellosaurus do not have db name
        if len(items) == 1:
            entity_type = "Cellosaurus"
            identifier = f"{entity_type}:{items[0]}"
        else:
            # quirk
            if items[0] == "CVCL_6412|CL":
                entity_type = "Cellosaurus"
                identifier = f"{entity_type}:CVCL_6412"
            else:
                entity_type = items[0]
                identifier = items[1]

        assert (
            entity_type is not None
        ), f"Could not determine type of annotation: {entity_type_identifier}"

        return (entity_type, identifier)

    def parse_figure_annotations(self, figure_annotations: list) -> list[Annotation]:
        """
        Parse annotations
        """

        assert (
            self.config.entity_type_map is not None
        ), "Multi-entity corpus must specify `entity_type_map`"

        annotations = []
        for a in figure_annotations:

            entity_type, _ = self.unpack_entity_type_identifier(a["identifiers"])

            kwargs = copy.deepcopy(a)
            kwargs.pop("identifiers")

            kwargs["entity_type"] = entity_type

            is_annotation = (
                self.config.entity_type_map.get(entity_type) == self.config.entity_type
            )
            is_foreign_annotation = (
                self.config.entity_type_map.get(entity_type)
                in self.config.foreign_entity_types
            )

            # annotation has same type as corpus: keep
            if is_annotation and entity_type in self.normalized_entities:
                kwargs["identifiers"] = a["identifiers"]

            elif is_foreign_annotation and self.config.add_foreign_annotations:
                kwargs["foreign"] = True

            # e.g. corpus.config.entity_type==gene and a["entity_type"] == Cellosaurus
            else:
                continue

            annotations.append(Annotation(**kwargs))

        return annotations

    def get_figure_caption_annotations(self, eid: str, figure: str) -> list[Annotation]:
        """
        Extract Figure caption annnotations
        """

        annotations = []

        example_annotations = self.eid_to_annotations.get(eid)

        if example_annotations is not None:

            figure_annotations = example_annotations.get(figure)

            if figure_annotations is not None:

                annotations = self.parse_figure_annotations(figure_annotations)

        return annotations

    def load_example(self, collection: bioc.BioCCollection) -> Example:
        """
        Aggregate all figure captions of a PMCID in a single document
        """

        passages = []

        offset = 0

        eid = None

        for idx, d in enumerate(collection.documents):

            assert (
                len(d.passages) == 1
            ), "Document contains more than one passage (figure caption). This is not expected!"

            if eid is None:
                eid = str(d.infons["pmc_id"])

            text = clean_text(d.passages[0].text)
            figure = d.infons["figure"]

            assert (
                eid is not None
            ), f"Could not determine example id from collection {next(collection.documents).id}"

            annotations = self.get_figure_caption_annotations(eid=eid, figure=figure)

            for a in annotations:
                a.text = clean_text(a.text)
                a.start += offset
                a.end += offset

            passage = Passage(
                id=idx,
                offset=offset,
                text=text,
                annotations=annotations,
                type=figure,
            )

            offset += len(text) + 1

            passages.append(passage)

        assert (
            eid is not None
        ), f"Could not determin example id in collection with documents: {[d.id for d in collection]}"
        example = Example(id=eid, passages=passages)

        return example

    def on_before_load(self, directory: str, kb: BelbKb):
        """
        Hook to perform operations before data is loaded
        """

        self.load_eid_to_annotations(directory=directory)

        if self.config.entity_type == Entities.GENE:
            self.load_uniprot_to_ncbi_gene(directory=directory)

    def load_split(self, directory: str, split: str) -> list[Example]:
        """
        Load examples in split
        """

        examples = []

        for eid in self.splits[str(split)]:

            path = os.path.join(directory, "BioIDtraining_2", "caption_bioc")

            c = bioc.load(os.path.join(path, f"{eid}.xml"))

            examples.append(self.load_example(c))

        return examples


def main(args: Namespace):
    """
    Standalone
    """

    options = CorpusConverter.extract_config_options_from_args(args)

    # TODO: use all entities
    # for entity_type in BioIdCorpusConfig.entity_types:
    for entity_type in [Entities.CELL_LINE]:

        kb = AutoBelbKb.from_name(
            name=ENTITY_TO_KB_NAME[entity_type],
            directory=args.dir,
            db_config=args.db,
            debug=args.debug,
        )

        config = BioIdCorpusConfig(entity_type=entity_type, **options)
        parser = BioIdCorpusParser(config=config)

        converter = CorpusConverter(
            directory=args.dir, parser=parser, kb=kb, pubtator=args.pubtator
        )
        converter.to_belb()
        if config.entity_type == Entities.GENE:
            logger.debug(
                "Uniprot -> NCBI Gene mapping - {} entries - {} found in corpus but not in mapping",
                len(parser.uniprot_to_ncbi_gene),
                len(parser.uniprot_no_mapping),
            )
