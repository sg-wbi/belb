#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base interfaces for data: example -> passage -> annotation
"""

import copy
import re
from dataclasses import dataclass, field
from typing import Optional, TypeVar, Union

import bioc
from bioc import pubtator
from loguru import logger

from belb.utils import StrEnum, compute_hexdigest, remove_quotes

NA = "-"

SYMBOL_CODE = 0

INVALID_POSITION = -1

OBSOLETE_IDENTIFIER = "-1"

IDENTIFIERS_CONNECTOR = ";"

ANNOTATION_IDENTIFYING_FIELDS = [
    "start",
    "end",
    "text",
    "entity_type",
    "identifiers",
    "location",
]

CHROMOSOMES = list(str(i) for i in range(1, 23)) + ["X", "Y", "MT"]


class Entities(StrEnum):

    """
    Container for entities
    """

    GENE = "gene"
    DISEASE = "disease"
    CHEMICAL = "chemical"
    SPECIES = "species"
    VARIANT = "variant"
    CELL_LINE = "cell_line"
    UMLS = "umls"


# https://www.python.org/dev/peps/pep-0673/#use-in-classmethod-signatures
SelfAnnotation = TypeVar("SelfAnnotation", bound="Annotation")
SelfPassage = TypeVar("SelfPassage", bound="Passage")
SelfExample = TypeVar("SelfExample", bound="Example")


@dataclass
class Annotation:
    """Annotation container"""

    start: int
    end: int
    text: str
    entity_type: str
    identifiers: Optional[Union[str, list]] = None
    location: Optional[str] = None
    foreign: bool = False
    id: Optional[int] = None
    infons: dict = field(default_factory=dict)
    original: Optional[dict] = None

    def __post_init__(self):
        if self.original is None:
            self.original = {
                k: copy.deepcopy(getattr(self, k, NA))
                for k in ANNOTATION_IDENTIFYING_FIELDS
            }

        if self.foreign:
            self.identifiers = NA

        assert (
            self.identifiers is not None
        ), "Annotation w/ foreign=False must provide identifiers!"

    def pack_identifiers(self, identifiers: list[str]) -> str:
        """
        Pack list of identifiers for writing
        """

        return f"{IDENTIFIERS_CONNECTOR}".join(str(i) for i in identifiers)

    @classmethod
    def from_pubtator(
        cls: type[SelfAnnotation], a: pubtator.PubTatorAnn
    ) -> SelfAnnotation:
        """Load annotation from BioCAnnotation"""

        kwargs: dict = {}
        kwargs["start"] = a.start
        kwargs["end"] = a.end
        kwargs["entity_type"] = a.type
        kwargs["identifiers"] = a.id
        kwargs["text"] = a.text

        return cls(**kwargs)

    @classmethod
    def from_bioc(cls: type[SelfAnnotation], a: bioc.BioCAnnotation) -> SelfAnnotation:
        """Load annotation from BioCAnnotation"""

        kwargs: dict = {}
        kwargs["start"] = a.total_span.offset
        kwargs["end"] = a.total_span.end
        kwargs["text"] = a.text
        kwargs["entity_type"] = a.infons.pop("type", "NA")
        kwargs["id"] = a.id

        if a.infons.get("foreign") is not None:
            kwargs["foreign"] = a.infons.pop("foreign")

        identifiers = a.infons.get("identifier")
        if identifiers is not None:
            kwargs["identifiers"] = identifiers.split(IDENTIFIERS_CONNECTOR)
            a.infons.pop("identifier")

        if a.infons.get("original") is not None:
            kwargs["original"] = a.infons.pop("original")

        kwargs["infons"] = a.infons

        return cls(**kwargs)

    def nested(self, other: "Annotation") -> bool:
        """
        Check if annotation is nested in `other`
        """
        return self.start >= other.start and self.end <= other.end

    def overlaps(self, other: "Annotation") -> bool:
        """
        Check if annotation is overlapping with `other`
        """
        return (self.start <= other.start < self.end) or (
            self.start < other.end <= self.end
        )

    def to_tuple(self, identifiers: bool = True) -> tuple:
        """
        Make annotation hashable -> tuple
        """

        key = [
            self.start,
            self.end,
            self.text,
            self.entity_type,
        ]

        if identifiers:
            key.append(
                self.pack_identifiers(self.identifiers)
                if isinstance(self.identifiers, list)
                else self.identifiers
            )

        return tuple(key)

    def _grouped_to_hexdigest(self, eid: str, hasher: str = "md5") -> list[str]:
        assert self.original is not None

        start = self.original["start"]
        end = self.original["end"]
        text = self.original["text"]
        entity_type = self.original["entity_type"]
        identifiers = self.original["identifiers"]
        location = self.original["location"]

        hexdigests: set = set()

        assert len(identifiers) == len(
            entity_type
        ), f"# original `identifiers` ({len(identifiers)}) !=  # original entity types ({len(entity_type)})"
        for e, i in zip(entity_type, identifiers):
            hexdigests.add(
                compute_hexdigest(
                    message=(eid, location, start, end, text, e, i), hasher=hasher
                )
            )
        assert len(hexdigests) == len(
            identifiers
        ), f"Collisions with `{hasher}`?! Annotation: {self.original}"

        return list(hexdigests)

    def _original_to_hexdigest(self, eid: str, hasher: str = "md5"):
        assert self.original is not None

        if isinstance(self.original["entity_type"], list) or isinstance(
            self.original["identifiers"], list
        ):
            hexdigests = self._grouped_to_hexdigest(eid=eid, hasher=hasher)

        else:
            hexdigests = [
                compute_hexdigest(
                    message=(
                        eid,
                        self.original["location"],
                        self.original["start"],
                        self.original["end"],
                        self.original["text"],
                        self.original["entity_type"],
                        self.original["identifiers"],
                    ),
                    hasher=hasher,
                )
            ]

        return hexdigests

    def to_hexdigest(
        self, eid: str, hasher: str = "md5", original: bool = True
    ) -> Union[str, list[str]]:
        """
        Represent annotation as hash.
        If `original` it can be a list, since annotations can be aggregated by entity_type/identifiers.
        """

        out = self._original_to_hexdigest(eid=eid, hasher=hasher)

        if not original:
            out = (
                compute_hexdigest(message=tuple(out), hasher=hasher)
                if len(out) > 1
                else out[0]
            )

        return out

    def to_belb(self, eid: str) -> bioc.BioCAnnotation:
        """Convert to BioCAnnotation"""

        biocann = bioc.BioCAnnotation()
        biocann.id = str(self.id)
        biocann.text = self.text

        infons: dict = {}
        infons["type"] = self.entity_type
        if not self.foreign:
            assert isinstance(
                self.identifiers, list
            ), f"Identifiers were not proccessed! Annotation: {self}"
            infons["identifier"] = self.pack_identifiers(self.identifiers)
        infons["foreign"] = self.foreign
        infons["hexdigest"] = self.to_hexdigest(eid=eid, original=False)
        infons["original"] = self.original

        infons.update(self.infons)

        biocann.infons = infons

        length = self.end - self.start

        locations = [bioc.BioCLocation(offset=self.start, length=length)]
        biocann.locations = locations

        return biocann


@dataclass
class Passage:
    """Passage container"""

    id: int
    offset: int
    text: str
    annotations: list[Annotation]
    type: str
    eid: str = "-1"

    def is_empty(self) -> bool:
        """
        Passage is empty if does not have annotations (excluding foreign)
        """

        return len([a for a in self.annotations if not a.foreign]) == 0

    def to_tuple(self) -> tuple[int, int, str, list[Annotation], str]:
        """Convert to tuple"""
        if self.eid == "-1":
            raise ValueError("Cannot convert passage to tuple with `eid=-1`!")
        return (self.id, self.offset, self.text, self.annotations, self.eid)

    @classmethod
    def from_bioc(cls: type[SelfPassage], passage: bioc.BioCPassage) -> SelfPassage:
        """Load from BioCPassage"""

        kwargs: dict = {}
        kwargs["id"] = int(passage.infons.get("id"))
        kwargs["offset"] = passage.offset
        kwargs["type"] = passage.infons.get("type")
        annotations = [Annotation.from_bioc(a) for a in passage.annotations]
        kwargs["annotations"] = annotations
        kwargs["text"] = passage.text

        return cls(**kwargs)

    def remap_annotation_offsets(
        self, eid: str, annotations: list[Annotation]
    ) -> list[Annotation]:
        """
        Compute now offsets for annotations
        """
        annotations = sorted(annotations, key=lambda x: x.start)

        remapped: list = []

        annotation_match_checks = [False] * len(annotations)

        last_match = 0

        for aidx, a in enumerate(annotations):
            # skip annotation if already used
            if annotation_match_checks[aidx]:
                continue

            pattern_str = re.escape(a.text)
            pattern_str = rf"(?<!\w){pattern_str}(?!\w)"
            pattern = re.compile(pattern_str)

            # from last match found in sentence
            # check for exact match of # of sentinel tokens
            match = re.search(pattern, self.text[last_match:])

            if match is not None:
                text_offset = len(self.text[:last_match])

                last_match = match.end() + text_offset

                a.start = match.start() + text_offset + self.offset
                a.end = match.end() + text_offset + self.offset

                remapped.append(a)

                annotation_match_checks[aidx] = True

        if not all(annotation_match_checks):
            unmatched = [
                a.text
                for idx, a in enumerate(annotations)
                if not annotation_match_checks[idx]
            ]
            logger.debug(
                f"EID:{eid}| Could not remap annotations: `{unmatched}` (probably unicode error)"
            )

        return remapped

    def to_belb(self, eid: str) -> bioc.BioCPassage:
        """Convert to BioCPassage"""

        passage = bioc.BioCPassage()
        passage.offset = self.offset
        passage.infons["id"] = self.id
        passage.infons["type"] = self.type
        passage.text = self.text
        passage.annotations = [a.to_belb(eid=eid) for a in self.annotations]

        return passage


@dataclass
class Example:
    """Example container"""

    id: str
    passages: list[Passage]
    identifiers: Optional[list[str]] = None

    def __post_init__(self):
        self.prepared = False
        self.passages_text_modified = False
        self.annotations_offsets_relative_to_passage = False
        for p in self.passages:
            p.eid = self.id

    def get_annotations_hexdigests(self) -> list[str]:
        """
        Return hashes of all annotations in example
        """

        hexdigests = set()
        for p in self.passages:
            for a in p.annotations:
                for h in a.to_hexdigest(eid=self.id):
                    if h in hexdigests:
                        breakpoint()
                        raise ValueError(f"Hash collision for annotation {a}")
                    hexdigests.add(h)

        return list(hexdigests)

    def reset_offsets(self):
        """
        Reset passages attributes: passages offsets,ids and annotations offsets
        """

        offset = 0

        for p in self.passages:
            p.offset = offset
            offset += len(p.text) + 1

            if self.annotations_offsets_relative_to_passage:
                for a in p.annotations:
                    a.start = a.start + p.offset
                    a.end = a.end + p.offset

        self.passages_text_modified = False
        self.annotations_offsets_relative_to_passage = False

    @property
    def is_empty(self) -> bool:
        """No passages"""

        no_passages = len(self.passages) == 0
        no_annotations = all(
            len([a for a in p.annotations if not a.foreign]) == 0 for p in self.passages
        )

        return no_passages or no_annotations

    @classmethod
    def from_bioc(cls: type[SelfExample], document: bioc.BioCDocument) -> SelfExample:
        """Load from BioCDocument"""
        kwargs: dict = {}

        kwargs["id"] = document.id

        passages = [Passage.from_bioc(p) for p in document.passages]
        for p in passages:
            p.eid = document.id
        kwargs["passages"] = passages

        identifiers = document.infons.get("identifiers")
        if identifiers is not None:
            kwargs["identifiers"] = identifiers.split(",")

        example = cls(**kwargs)
        example.prepared = True

        return example

    def filter_annotations(self, hexdigests: set[str]):
        """
        Keep only annotations present in list of annotations SHA256 hashes
        """

        for p in self.passages:
            annotations = []
            for a in p.annotations:
                for h in a.to_hexdigest(eid=self.id):
                    if h in hexdigests:
                        annotations.append(a)
            p.annotations = annotations

    def drop_duplicate_annotations(self):
        """
        Check that there are no duplicate annotations (start,end,text,type,identifiers)
        """

        seen = set()

        for p in self.passages:
            annotations = []
            for a in sorted(p.annotations, key=lambda x: x.start):
                key = a.to_tuple()
                if key not in seen:
                    annotations.append(a)
                    seen.add(key)
                else:
                    logger.debug(
                        "EID:{} - Remove duplicate annotation {}", self.id, key
                    )

            p.annotations = annotations

    def group_annotations_by_span(self):
        """
        Replace multiple identifical annotations but for the identifiers
        w/ single annotation w/ comma-separated list of identifiers
        """

        for p in self.passages:
            # group annotation by span
            grouped_annotations = {}
            for a in p.annotations:
                key = a.to_tuple(identifiers=False)
                if key not in grouped_annotations:
                    grouped_annotations[key] = []
                grouped_annotations[key].append(a)

            # replace multiple annotation w/ different identifiers
            # with single annotation and comma-separated list of identifiers
            for key, annotations in grouped_annotations.items():
                if len(annotations) > 1:
                    unique_a = annotations[0]
                    unique_a.original["entity_type"] = [
                        a.original["entity_type"] for a in annotations
                    ]
                    unique_a.original["identifiers"] = [
                        a.original["identifiers"] for a in annotations
                    ]
                    assert (
                        len(set(a.foreign for a in annotations)) == 1
                    ), f"EID:{self.id} | Duplicate annotation has multiple values for `foreign` field!"
                    grouped_annotations[key] = unique_a
                    logger.debug(
                        "EID:{} | grouped annotations differening only in identifiers: `{}` ({})...",
                        self.id,
                        key,
                        unique_a.original["identifiers"],
                    )
                else:
                    grouped_annotations[key] = annotations[0]

            p.annotations = list(grouped_annotations.values())

    def prepare(self):
        """Set annotation attributes: id, nested, parent annotation"""

        self.drop_duplicate_annotations()

        self.group_annotations_by_span()

        idx = 0
        for p in self.passages:
            for a in sorted(p.annotations, key=lambda x: x.start):
                a.id = idx
                idx += 1

        self.prepared = True

    def inject_foreign_annotations(self, foreign_annotations: list[Annotation]):
        """
        Inject helper annotations
        """

        original_annotations = [a for p in self.passages for a in p.annotations]

        for fa in foreign_annotations[:]:
            for a in original_annotations:
                if any(
                    [
                        fa.overlaps(a),
                        fa.nested(a),
                        (fa.start, fa.end) == (a.start, a.end),
                        fa.text == a.text,
                    ]
                ):
                    foreign_annotations.remove(fa)
                    break

        for p in self.passages:
            annotations = [
                a
                for a in foreign_annotations
                if a.start >= p.offset and a.end <= p.offset + len(p.text)
            ]

            annotations = p.remap_annotation_offsets(
                eid=self.id, annotations=annotations
            )

            p.annotations += annotations

    @classmethod
    def from_text_and_annotations(
        cls: type[SelfExample], eid: str, text: dict, annotations: list[Annotation]
    ) -> SelfExample:
        """
        Create example from raw data
        """
        kwargs: dict = {}
        kwargs["id"] = eid
        kwargs["passages"] = []

        offset = 0

        for idx, (passage_type, passage_text) in enumerate(text.items()):
            passage_annotations = [
                a
                for a in annotations
                if a.start >= offset and a.end <= offset + len(passage_text)
            ]

            passage = Passage(
                id=idx,
                offset=offset,
                text=passage_text,
                annotations=passage_annotations,
                type=passage_type,
            )

            kwargs["passages"].append(passage)

            offset += len(passage_text) + 1

        example = cls(**kwargs)

        return example

    def to_belb(self) -> bioc.BioCDocument:
        """Convert to BioCDocument"""

        if not self.prepared:
            raise RuntimeError(
                "Cannot convert to `BioC` format w/o first calling `prepare`!"
            )

        document = bioc.BioCDocument()
        document.id = str(self.id)

        if self.identifiers is not None:
            document.infons["identifiers"] = ",".join(
                [str(i) for i in self.identifiers]
            )

        for passage in self.passages:
            document.passages.append(passage.to_belb(eid=self.id))

        return document


class DictMixin:
    """
    Gives access to attributes in dataclass

    """

    @property
    def keys(self) -> tuple:
        """
        Values for table header
        """

        keys = tuple(k for k, v in sorted(self.__dict__.items()) if v is not None)

        return keys

    @property
    def values(self) -> tuple:
        """
        Values for upload to sqlite db
        """

        values = tuple(v for k, v in sorted(self.__dict__.items()) if v is not None)

        return values


@dataclass(frozen=True)
class Entry(DictMixin):
    """
    Container for kb entry
    """

    uid: int
    identifier: int
    name: str
    description: int
    foreign_identifier: Optional[int] = None
    attribute: str = NA

    def __post_init__(self):
        name = self.name.replace("\t", " ")
        # 10013568:C2347166|ENG|S|L0028429|PF|S9297297|Y|A23967270||C68860||NCI_NCI-HL7|AB|12879|'|0|N|256|
        if name != "'" or name != '"':
            # 3405153:-       13      2867241 "Qipengyuania aestuarii' Liu et al. 2022        3405151
            name = remove_quotes(name)
        super().__setattr__("name", name)


@dataclass(frozen=True)
class HistoryEntry(DictMixin):
    """
    Container for history entry
    """

    old_identifier: str
    new_identifier: str


@dataclass(frozen=True)
class IdentifierMappingEntry(DictMixin):
    """
    Container for identifier mapping entry
    """

    original_identifier: str
    internal_identifier: int


@dataclass(frozen=True)
class CitationEntry(DictMixin):
    """
    Container for citation entry
    """

    pmid: int
    identifier: str


@dataclass(frozen=True)
class ForeignIdentifierEntry(DictMixin):
    """
    Container for foreign identifier entry
    """

    identifier: int
    name: Optional[str] = None
