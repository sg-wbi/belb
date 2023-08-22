#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Interface to OSIRIS (v1.2) corpus
Inspired by: https://github.com/bigscience-workshop/biomedical/blob/master/biodatasets/osiris/osiris.py
"""

import os
import xml.etree.ElementTree as ET
from argparse import Namespace

from belb.corpora.corpus import CorpusConverter
from belb.corpora.parser import (BaseBelbCorpusConfig, BaseBelbCorpusParser,
                                 Splits)
from belb.kbs import ENTITY_TO_KB_NAME, AutoBelbKb
from belb.preprocessing.data import NA, Annotation, Entities, Example
from belb.resources import Corpora

INVALID_IDENTIFIERS = ["No"]


def safe_find(elem: ET.Element, value: str):
    """
    Raise error if element value is None
    """

    out = elem.find(value)

    if out is None:
        raise ValueError(f"Element {elem} has no value: `{value}`!")

    return out


class OsirisCorpusConfig(BaseBelbCorpusConfig):
    """
    Osiris default configuration
    """

    resource = Corpora.OSIRIS
    splits = [Splits.TEST]
    entity_types = [Entities.VARIANT, Entities.GENE]
    entity_type = Entities.VARIANT
    foreign_entity_types = [Entities.GENE]
    native_foreign_annotations = True
    pmc = False
    local = False
    add_foreign_annotations = True
    title_abstract = True
    entity_type_map = {"variant": Entities.VARIANT, "gene": Entities.GENE}

    def __init__(self, **kwargs):
        if kwargs.get("entity_type") == Entities.VARIANT:
            kwargs["foreign_entity_types"] = [Entities.GENE]

        elif kwargs.get("entity_type") == Entities.GENE:
            kwargs["foreign_entity_types"] = [Entities.SPECIES]
            kwargs["native_foreign_annotations"] = False
        super().__init__(**kwargs)


class OsirisCorpusParser(BaseBelbCorpusParser):
    """Interface OSIRIS corpus"""

    def parse_annotation_identifiers(self, original_identifiers: str) -> list[str]:
        """Expand identifiers to list, apply mapping"""

        ids_list = original_identifiers.split(",")

        annotation_ids = [NA if i in INVALID_IDENTIFIERS else i for i in ids_list]

        return annotation_ids

    def extract_text_and_annotations(
        self, annotated_elem: ET.Element
    ) -> tuple[str, list]:
        """
        Extract text and in-text tags (annotations) from xml element
        """

        annotations = []

        for a in annotated_elem:
            annotation: dict = {"entity_type": a.tag, "text": a.text}
            annotation.update(a.attrib)
            annotations.append(annotation)

        annotations_text = [a["text"] for a in annotations]
        annotation_idx = 0

        text = ""
        for text_chunk in annotated_elem.itertext():
            if text_chunk in annotations_text[annotation_idx:]:
                annotation = annotations[annotation_idx]
                annotation["start"] = len(text)
                annotation["end"] = annotation["start"] + len(text_chunk)
                annotation_idx += 1
            text += text_chunk

        assert annotation_idx == len(
            annotations
        ), "Not all annotations were found when computing offsets"

        return text, annotations

    def parse_article_element(self, article_element: ET.Element) -> dict:
        """
        Parse article in xml format
        """

        article: dict = {}

        pmid_elem = safe_find(elem=article_element, value="Pmid")

        title_elem = safe_find(elem=article_element, value="Title")

        title, title_annotations = self.extract_text_and_annotations(title_elem)

        abstract_elem = safe_find(elem=article_element, value="Abstract")

        abstract, abstract_annotations = self.extract_text_and_annotations(
            abstract_elem
        )

        # make annotations offsets in abstract relative to whole text
        for a in abstract_annotations:
            a["start"] += len(title) + 1
            a["end"] += len(title) + 1

        article["pmid"] = pmid_elem.text
        article["title"] = title
        article["abstract"] = abstract
        article["annotations"] = title_annotations + abstract_annotations

        return article

    def get_annotations_from_article(self, article: dict) -> list[Annotation]:
        """
        Build abstract annotations
        """

        assert (
            self.config.entity_type_map is not None
        ), "Multi-entity corpus must specify `entity_type_map`"

        short_entity_type = str(self.config.entity_type).lower()[0]

        annotations = []

        for a in article["annotations"]:
            kwargs = {
                "start": a["start"],
                "end": a["end"],
                "text": a["text"],
                "entity_type": a["entity_type"],
            }

            is_annotation = (
                self.config.entity_type_map.get(a["entity_type"])
                == self.config.entity_type
            )

            is_foreign_annotation = (
                self.config.entity_type_map.get(a["entity_type"])
                in self.config.foreign_entity_types
            )

            if is_annotation:
                kwargs["identifiers"] = a[f"{short_entity_type}_id"]

            elif is_foreign_annotation and self.config.add_foreign_annotations:
                kwargs["foreign"] = True

            else:
                continue

            annotations.append(Annotation(**kwargs))

        return annotations

    def load_example(self, article: dict) -> Example:
        """
        Load example
        """

        annotations = self.get_annotations_from_article(article)

        text = {f: article.get(f) for f in ["title", "abstract"]}

        example = Example.from_text_and_annotations(
            eid=article["pmid"], text=text, annotations=annotations
        )

        return example

    def load_split(self, directory: str, split: str) -> list[Example]:
        """
        Load all examples in test split
        """

        filepath = os.path.join(directory, "corpus.xml")

        xml_articles = ET.parse(filepath).getroot()

        examples = []

        for xml_article in list(xml_articles):
            article = self.parse_article_element(xml_article)

            example = self.load_example(article)

            examples.append(example)

        return examples


def main(args: Namespace):
    """
    Standalone
    """

    # species_subsets = load_species_subsets()
    options = CorpusConverter.extract_config_options_from_args(args)
    # TODO: use all entities
    for entity_type in [Entities.VARIANT]:
        kb = AutoBelbKb.from_name(
            name=ENTITY_TO_KB_NAME[entity_type],
            directory=args.dir,
            db_config=args.db,
            debug=args.debug,
        )

        config = OsirisCorpusConfig(entity_type=entity_type, **options)
        parser = OsirisCorpusParser(config=config)
        converter = CorpusConverter(
            directory=args.dir, parser=parser, kb=kb, pubtator=args.pubtator
        )
        converter.to_belb()
