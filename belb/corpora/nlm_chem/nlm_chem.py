#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Interface for NLMChem corpus
"""

import os
from argparse import Namespace

import bioc

from belb.corpora.corpus import CorpusConverter
from belb.corpora.parser import (BaseBelbCorpusParser, BaseBelbCorpusConfig,
                                 Splits, qaqcmethod)
from belb.kbs import BelbKb, BelbKbSchema, CtdChemicalsKbConfig
from belb.preprocessing.data import NA, Annotation, Entities, Example, Passage
from belb.resources import Corpora

INVALID_IDENTIFIERS = ["-"]


class NlmChemCorpusConfig(BaseBelbCorpusConfig):
    """
    NLM-Chem default configuration
    """

    resource = Corpora.NLM_CHEM
    splits = [Splits.TRAIN, Splits.DEV, Splits.TEST]
    entity_type = Entities.CHEMICAL
    entity_types = [Entities.CHEMICAL]
    pmc = True
    local = False
    title_abstract = True


class NlmChemCorpusParser(BaseBelbCorpusParser):
    """Interface NLMChem corpus"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.split_files: dict = {
            Splits.DEV: "BC7T2-NLMChem-corpus-dev.BioC.xml",
            Splits.TEST: "BC7T2-NLMChem-corpus-test.BioC.xml",
            Splits.TRAIN: "BC7T2-NLMChem-corpus-train.BioC.xml",
        }

    @qaqcmethod
    def handle_errors_annotation_offsets(
        self, eid: str, a: Annotation, p: Passage  # pylint: disable=unused-argument
    ):
        """Fix annotation offsets"""

        if eid == "5600090" and (a.start, a.end) == (28869, 28874):
            a.start += 1
            a.end += 1

        if eid == "5096026" and a.text == "ruthenium polypyridy":
            a.end += 1
            a.text = "ruthenium polypyridyl"

    def parse_annotation_identifiers(self, original_identifiers: str) -> list[str]:
        """Expand identifiers to list, apply mapping"""

        ids_list = original_identifiers.split(",")

        annotation_ids = []
        for i in ids_list:
            # 2276181
            # <annotation id="G30">
            #   <infon key="type">Chemical</infon>
            #   <infon key="identifier">-</infon>
            #   <location offset="7990" length="9"/>
            #   <text>RPMI-1640</text>
            # </annotation>
            if i in INVALID_IDENTIFIERS:
                i = NA
            annotation_ids.append(i)
        return annotation_ids

    def build_passages(
        self,
        texts: list[str],
        types: list[str],
        annotations_lists: list[list[Annotation]],
    ) -> list[Passage]:
        """
        Create passages from parsed elements
        """

        assert len(texts) == len(
            annotations_lists
        ), f"# of passages ({len(texts)}) != # of per-passage annotations ({len(annotations_lists)})!"

        passages = []

        offset = 0

        for idx, (text, type_, annotations) in enumerate(
            zip(texts, types, annotations_lists)
        ):
            passage = Passage(
                id=idx,
                offset=offset,
                text=text,
                type=type_,
                annotations=annotations,
            )

            offset += len(text) + 1

            passages.append(passage)

        return passages

    def load_example(self, document: bioc.BioCDocument) -> Example:
        """
        Load example from raw file

        Parameters
        ----------
        document : bioc.BioCDocument
            Example in BioC format

        Returns
        -------
        Example
            Internal representation

        Raises
        ------
        RuntimeError:
            Failure in splitting text into passsages
        """

        texts: list[str] = []
        annotations: list[list[Annotation]] = []
        types: list[str] = []

        text_total_length = 0

        for _, p in enumerate(document.passages):

            types.append(p.infons.get("type"))

            offset = p.offset - text_total_length

            text_total_length += len(p.text) + 1

            texts.append(p.text)

            passage_annotations = []

            for a in p.annotations:

                entity_type = a.infons.get("type")
                # no in-text annotation
                if entity_type in ["MeSH_Indexing_Chemical", "OTHER"]:
                    continue

                start = a.total_span.offset - offset
                end = a.total_span.end - offset

                original_identifiers = a.infons.get("identifier")
                text = a.text

                a = Annotation(
                    start=start,
                    end=end,
                    text=text,
                    identifiers=original_identifiers,
                    entity_type=entity_type,
                )

                passage_annotations.append(a)

            annotations.append(passage_annotations)

        passages = self.build_passages(
            texts=texts, annotations_lists=annotations, types=types
        )

        example = Example(id=document.id, passages=passages)

        original_text = " ".join([p.text for p in document.passages])
        example_text = " ".join([p.text for p in example.passages])

        if len(example_text) != len(original_text):
            raise RuntimeError(f"EID:{example.id}: Failed parsing full text article")

        return example

    def load_split(self, directory: str, split: str) -> list[Example]:

        examples = []

        collection = bioc.load(os.path.join(directory, self.split_files[split]))

        for document in collection.documents:

            example = self.load_example(document)

            examples.append(example)

        return examples


def main(args: Namespace):
    """
    Standalone
    """

    schema = BelbKbSchema(db_config=args.db, kb_config=CtdChemicalsKbConfig())
    kb = BelbKb(directory=args.dir, schema=schema)
    options = CorpusConverter.extract_config_options_from_args(args=args)
    config = NlmChemCorpusConfig(**options)
    parser = NlmChemCorpusParser(config=config)
    converter = CorpusConverter(
        directory=args.dir, parser=parser, kb=kb, pubtator=args.pubtator
    )
    converter.to_belb()
