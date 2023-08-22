#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Create NLM Gene corpus
"""

import os
from argparse import Namespace

import bioc

from belb.corpora.corpus import CorpusConverter
from belb.corpora.parser import (BaseBelbCorpusParser, BaseBelbCorpusConfig,
                                 Splits, qaqcmethod)
from belb.kbs import BelbKb, BelbKbSchema, NcbiGeneKbConfig
from belb.preprocessing.data import (INVALID_POSITION, NA, Annotation,
                                     Entities, Example, Passage)
from belb.resources import Corpora

INVALID_IDENTIFIERS = ["-1", "-"]

ERROR_ANNOTATIONS = [
    ("24586582", "p21(Cip1"),
    ("21172391", "N-methyl-D-aspartate recepto"),
    ("29956586", "nonvoltage-dependent channe"),
    (
        "27825100",
        "master regulators of oxidative metabolism transcription factor A mitochondria",
    ),
    ("23460868", "β-ca"),
    ("23110133", "d"),
    ("26620890", "F480("),
    ("28036337", "α-Sma,"),
    ("24402617", "uclear factor-κB"),
    ("22144572", "nterferon-γ"),
    ("20459769", "AKT,"),
]


class NlmGeneCorpusConfig(BaseBelbCorpusConfig):
    """
    NLM-Gene default configuration
    """

    resource = Corpora.NLM_GENE
    splits = [Splits.TRAIN, Splits.DEV, Splits.TEST]
    entity_type = Entities.GENE
    entity_types = [Entities.GENE]
    pmc = False
    title_abstract = True
    local = False
    add_foreign_annotations = True
    foreign_entity_types = [Entities.SPECIES, Entities.CELL_LINE]


class NLMGeneCorpusParser(BaseBelbCorpusParser):
    """Interface BC5CDR corpus"""

    @qaqcmethod
    def handle_errors_annotation_text(self, eid: str, a: Annotation, p: Passage):
        """Fix annotation text"""

        a.text = a.text.replace("\xa0", " ")

        if eid == "27798105" and a.text == " methyltransferase":
            a.text = p.text[a.start : a.end]

        if eid == "23648511" and a.text == "AP-2b":
            a.text = "AP-2ß"

        if eid == "24586582" and (a.start, a.end) == (894, 900):
            a.end -= 1
            a.text = p.text[a.start - p.offset : a.end - p.offset]
            # a.text = p.text[a.start : a.end]

        if eid == "28259970" and a.text == "E cadherin":
            a.text = p.text[a.start - p.offset : a.end - p.offset]
            # a.text = p.text[a.start : a.end]

        if eid in ["28112376", "28138708", "28112366", "23934545"]:
            a.text = p.text[a.start - p.offset : a.end - p.offset]
            # a.text = p.text[a.start : a.end]

        annotation_text_by_offset = p.text[a.start - p.offset : a.end - p.offset]
        if any(chr in annotation_text_by_offset for chr in ["α", "β", "γ"]):
            a.text = annotation_text_by_offset
            # a.text = p.text[a.start : a.end]

    def parse_annotation_identifiers(self, original_identifiers: str) -> list[str]:
        """Expand identifiers to list, apply mapping"""

        ids_list = (
            original_identifiers.strip().replace(",", ";").replace("|", ";").split(";")
        )

        annotation_ids = [NA if i in INVALID_IDENTIFIERS else i for i in ids_list]

        return annotation_ids

    def load_example(self, document: bioc.BioCDocument) -> Example:
        """
        Load example from raw files

        Parameters
        ----------
        document : bioc.BioCDocument
            Original example

        Returns
        -------
        Example
            Internal representation
        """

        passages = []
        offset = 0

        for idx, p in enumerate(document.passages):

            annotations = []

            for a in p.annotations:

                entity_type = a.infons.get("type")
                if entity_type in ["Domain", "Other"]:
                    # print(f'NO GENE (NO IDENTIFIER): {a}')
                    continue

                # Corpus/FINAL/27110092.BioC.XML
                # <annotation id="3">
                # <infon key="NCBI Gene identifier">-</infon>
                # <infon key="type">Gene</infon>
                # <location offset="617" length="9"/>
                # <text>cytokines</text>
                # </annotation>
                identifiers = a.infons.get("NCBI Gene identifier", "-1")
                if (document.id, a.text) in ERROR_ANNOTATIONS:
                    a.start = INVALID_POSITION
                    a.end = INVALID_POSITION

                a = Annotation(
                    start=a.total_span.offset,
                    end=a.total_span.end,
                    text=a.text,
                    identifiers=identifiers,
                    entity_type=entity_type,
                )

                annotations.append(a)

            passage = Passage(
                id=idx,
                offset=offset,
                text=p.text.replace("\xa0", " "),
                annotations=annotations,
                type=p.infons.get("type", NA),
            )

            offset = len(passage.text) + 1

            passages.append(passage)

        example = Example(id=document.id, passages=passages)

        return example

    def load_split(self, directory: str, split: str) -> list[Example]:
        """
        Load split
        """

        texts_dir = os.path.join(directory, "Corpus", "FINAL")

        examples = []

        for i in self.splits[split]:

            collection = bioc.load(os.path.join(texts_dir, f"{i}.BioC.XML"))

            example = self.load_example(collection.documents[0])

            examples.append(example)

        return examples


def main(args: Namespace):
    """
    Standalone
    """

    options = CorpusConverter.extract_config_options_from_args(args=args)

    schema = BelbKbSchema(db_config=args.db, kb_config=NcbiGeneKbConfig())
    kb = BelbKb(directory=args.dir, schema=schema)

    config = NlmGeneCorpusConfig(**options)
    parser = NLMGeneCorpusParser(config=config)
    converter = CorpusConverter(
        directory=args.dir, parser=parser, kb=kb, pubtator=args.pubtator
    )
    converter.to_belb()
