#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base transformation interface
"""
import re
from abc import ABCMeta, abstractmethod

import wrapt

from belb.preprocessing.data import Annotation, Example, Passage

TRANSFORMATIONS = ["iwm", "sentences", "markers"]
UNICODE_SPACE_CHARS = re.compile(r"[\u2000-\u200B]")


@wrapt.decorator
def transformmethod(wrapped, instance, args, kwargs):  # pylint: disable=unused-argument
    """
    Decorator to check that Example input to `apply` and `safe_apply` is `prepared`
    """

    for arg in args:
        if isinstance(arg, Example):
            assert arg.prepared, "You need to call `example.prepare()` first!"

    for _, v in kwargs.items():
        if isinstance(v, Example):
            assert v.prepared, "You need to call `example.prepare()` first!"

    example = wrapped(*args, **kwargs)

    if (
        example.passages_text_modified
        or example.annotations_offsets_relative_to_passage
    ):
        example.reset_offsets()
        example.passages_text_modified = False
        example.annotations_offsets_relative_to_passage = False

    return example


class BaseTransformation(metaclass=ABCMeta):
    """
    Base interface for `Example` transformation
    """

    def __init__(self, allow_drop: bool = False):
        self.allow_drop = allow_drop

    @abstractmethod
    def apply(self, example: Example) -> Example:
        """
        Apply transformation
        """

    @abstractmethod
    def safe_apply(self, example: Example) -> Example:
        """
        Apply transformation w/ sanity checks
        """

    def handle_error(self, eid: str, msg: str) -> Example:
        """
        Handle transformation error: raise error or return empty example
        """
        if self.allow_drop:
            example = Example(id=eid, passages=[])
        else:
            raise RuntimeError(msg)

        return example

    def group_annotations_by_span(
        self, annotations: list[Annotation]
    ) -> list[list[Annotation]]:
        """
        Group annotations in buckets according to the span of text they cover.
        A bucket w/ length > 1 comprises nested and/or overlapping annotations.
        """

        assert len(annotations) > 0, "Cannot group an empty list!"

        # sort nested annotations by start
        # if same start get longest first
        # this way multiple markers (e.g. [ME]) at the same position will be
        # ordered by level of nesting
        annotations = sorted(annotations, key=lambda x: (x.start, -1 * len(x.text)))

        buckets = []
        bucket = [annotations[0]]

        for a in annotations[1:]:
            if any((a.nested(o) or a.overlaps(o)) for o in bucket):
                bucket.append(a)
            else:
                buckets.append(bucket)
                bucket = []
                bucket.append(a)

        buckets.append(bucket)

        return buckets

    def is_independent_annotation(self, grouped_annotations: list) -> bool:
        """
        After `group_annotations_by_span` check if it is an annotation group
        i.e. multiple annotations spanning same text span
        """

        return len(grouped_annotations) == 0 or len(grouped_annotations) == 1

    def get_text_span_multi_annotation(
        self, passage: Passage, annotations: list[Annotation]
    ) -> tuple[list[tuple[str, list]], int]:
        """
        Get the text span (by chars) spanned by multiple annotations
        """

        span_start = min(a.start - passage.offset for a in annotations)
        span_end = max(a.end - passage.offset for a in annotations)

        # assign to each characters the annotations they belong to
        chars: list = [(c, []) for c in list(passage.text[span_start:span_end])]

        for a in annotations:
            relative_start = a.start - passage.offset - span_start
            relative_end = relative_start + len(a.text)
            for i in range(relative_start, relative_end):
                chars[i][1].append(a.id)

        return chars, span_start

    def remap_offsets_grouped_annotations(
        self,
        annotations: list[Annotation],
        span_text: str,
        offset: int,
    ):
        """
        Re-compute offset of grouped annotation relative to the text span
        """

        for a in annotations:
            match = re.search(re.escape(a.text), span_text)
            if match is None:
                raise RuntimeError(
                    f"Cannot match overlapping/nested annotation {a} in corresponding text span `{span_text}`"
                )
            a.start = match.start() + offset
            a.end = a.start + len(a.text)

    def clean_text(self, text: str) -> str:
        """
        Standarsize white spaces
        """
        text = re.sub(
            UNICODE_SPACE_CHARS, " ", text
        )  # replace unicode space characters!
        text = text.replace("\xa0", " ")  # replace non-break space

        return text
