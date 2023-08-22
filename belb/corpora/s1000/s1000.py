#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Interface for S800 corpus
"""

import copy
import os
from argparse import Namespace

from bioc import pubtator

from belb.corpora.corpus import CorpusConverter
from belb.corpora.parser import (BaseBelbCorpusParser, BaseBelbCorpusConfig,
                                 Splits, qaqcmethod)
from belb.kbs import BelbKb, BelbKbSchema, NcbiTaxonomyKbConfig
from belb.preprocessing.data import Annotation, Entities, Example, Passage
from belb.resources import Corpora
from belb.utils.pubtator_central import PubTatorAPI


class S1000CorpusConfig(BaseBelbCorpusConfig):
    """
    S800 default configuration
    """

    resource = Corpora.S1000
    splits = [Splits.TRAIN, Splits.DEV, Splits.TEST]
    # splits = [Splits.TRAIN]
    entity_type = Entities.SPECIES
    entity_types = [Entities.SPECIES]
    pmc = False
    local = False
    entity_type_map = {
        "Genus": Entities.SPECIES,
        "Species": Entities.SPECIES,
        "Strain": Entities.SPECIES,
    }


class S1000CorpusParser(BaseBelbCorpusParser):
    """Interface to S1000 corpus"""

    # @qaqcmethod
    # def handle_errors_annotation_offsets(
    #     self, eid: str, a: Annotation, p: Passage  # pylint: disable=unused-argument
    # ):

    def try_fix_offset_errors(self, a: Annotation, p: Passage):

        annotated_text = p.text[a.start - p.offset : a.end - p.offset]

        if a.text != annotated_text:

            for shift in (1, 2, 3, 4, 5, -1, -2, -3, -4, -5):
                shifted_start = a.start - p.offset + shift
                shifted_end = a.end - p.offset + shift
                if a.text == p.text[shifted_start:shifted_end]:
                    a.start = a.start + shift
                    a.end = a.end + shift
                    a.text = p.text[shifted_start:shifted_end]
                    break

    def fetch_split_texts(self, directory: str, pmids: set):
        """
        Get abstracts text via PubTator API
        """

        pubtator_api = PubTatorAPI(
            download_history_file=os.path.join(directory, "cache.txt"),
            doc_format="pubtator",
        )

        for d in pubtator_api.fetch(pmids):

            with open(os.path.join(directory, f"{d.pmid}.txt"), "w") as fp:

                pubtator.dump([d], fp)

    def load_standoff_annotations(self, path: str) -> list[Annotation]:
        """
        Load annotation in standoff format
        """

        raw: dict = {}
        with open(path) as fp:
            for line in fp:
                aid, data = line.strip().split("\t", maxsplit=1)
                if aid.startswith("T"):
                    entity_type_locations, text = data.split("\t")
                    entity_type, locations = entity_type_locations.split(
                        " ", maxsplit=1
                    )

                    if entity_type not in self.config.entity_type_map:
                        raise ValueError(f"New entity type {entity_type}")
                    # one annotation can have muiltiple (start, end): 20970456.ann
                    locations = [
                        (int(loc.split(" ")[0]), int(loc.split(" ")[1]))
                        for loc in locations.split(";")
                    ]
                    raw[aid] = {
                        "locations": locations,
                        "entity_type": entity_type,
                        "text": text,
                    }
                elif aid.startswith("N"):
                    data = data.split("\t")[0]
                    _, ref, identifiers = data.split(" ")
                    assert (
                        ref in raw
                    ), f"Normalization lines references {ref} but it was never parsed"
                    raw[ref]["identifiers"] = identifiers

        annotations = [
            Annotation(
                **{
                    "text": ra["text"],
                    "entity_type": ra["entity_type"],
                    "identifiers": ra["identifiers"],
                    "start": loc[0],
                    "end": loc[1],
                }
            )
            for _, ra in raw.items()
            for loc in ra["locations"]
            # Not all annotations are normalized: 20962084.ann
            if ra.get("identifiers") is not None
        ]

        return annotations

    def load_split(self, directory: str, split: str) -> list[Example]:
        """
        Load split
        """

        path = os.path.join(directory, "S1000-corpus", "entire-corpus", str(split))

        pmids = set(
            f.replace(".ann", "") for f in os.listdir(path) if f.endswith(".ann")
        )

        self.fetch_split_texts(directory=path, pmids=pmids)

        examples = []

        for pmid in pmids:

            annotations = self.load_standoff_annotations(
                os.path.join(path, f"{pmid}.ann")
            )

            with open(os.path.join(path, f"{pmid}.txt")) as fp:
                # there is only one document per file
                document = pubtator.load(fp)[0]
                text = {"title": document.title, "abstract": document.abstract}
                # some documents may not have an abstract: 687594
                text = {k: v for k, v in text.items() if v is not None}

            example = Example.from_text_and_annotations(
                eid=pmid, text=text, annotations=annotations
            )

            examples.append(example)

        from belb.preprocessing.qaqc import OffsetError, test_offsets

        with open(os.path.join(os.getcwd(), "s1000_offset_errors.txt"), "a") as fp:
            for e in examples[:]:
                try:
                    test_offsets(e)
                except OffsetError:
                    for p in e.passages:
                        for a in p.annotations:
                            self.try_fix_offset_errors(a=a, p=p)
                    try:
                        test_offsets(e)
                    except OffsetError as error:
                        examples.remove(e)
                        fp.write(f"{split} - {e.id}\n")
                        # print(error)

        return examples

    def parse_annotation_identifiers(self, original_identifiers: str) -> list[str]:
        """Expand identifiers to list, apply mapping"""

        annotation_ids = [original_identifiers.split(":")[1]]

        return annotation_ids


def main(args: Namespace):
    """
    Standalone
    """

    schema = BelbKbSchema(db_config=args.db, kb_config=NcbiTaxonomyKbConfig())
    kb = BelbKb(directory=args.dir, schema=schema)
    options = CorpusConverter.extract_config_options_from_args(args)
    config = S1000CorpusConfig(**options)
    parser = S1000CorpusParser(config=config)
    converter = CorpusConverter(
        directory=args.dir, parser=parser, kb=kb, pubtator=args.pubtator
    )
    converter.to_belb()
