#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Interface to mark annotation IN text
"""

from collections import defaultdict

from belb.preprocessing import qaqc
from belb.preprocessing.data import Annotation, Entities, Example, Passage
from belb.preprocessing.transform import BaseTransformation, transformmethod

MARKERS = {
    "mention": {"start": "[MS]", "end": "[ME]"},
    "foreign": {"start": "[FS]", "end": "[FE]"},
    # Entities.SPECIES: {"start": "[BS]", "end": "[ES]"},
    # Entities.CELL_LINE: {"start": "[BCL]", "end": "[ECL]"},
}


class AddMentionMarkers(BaseTransformation):
    """
    Transformation to inject in text mentions markup
    """

    def __init__(self, entity_type: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.markers = MARKERS
        self.entity_type = entity_type

    def get_markers(self, entity_type: str) -> tuple:
        """
        Determine type of marker
        """

        if entity_type == self.entity_type:
            start_marker = self.markers["mention"]["start"]
            end_marker = self.markers["mention"]["end"]
        else:
            start_marker = self.markers["foreign"]["start"]
            end_marker = self.markers["foreign"]["end"]
            # start_marker = self.markers[entity_type]["start"]
            # end_marker = self.markers[entity_type]["end"]

        start_marker = f"{start_marker} "
        end_marker = f" {end_marker}"

        return start_marker, end_marker

    def get_grouped_annotations_inserts(
        self,
        annotations: list[Annotation],
        chars: list[tuple[str, list]],
        span_offset: int,
        passage_offset: int,
    ) -> dict[int, list]:
        """
        Determine where to insert mention markers relative to the text span
        """

        inserts: dict[int, list] = defaultdict(list)

        at_beginning: list = []

        for a in annotations:

            start_marker, end_marker = self.get_markers(a.entity_type)

            start = a.start - passage_offset - span_offset
            idx = start - 1

            # idx can be -1, this means prepending [MS]
            # at the beginning of the text span:
            # nested annotations starting at the same position
            if idx == -1:
                # most outer annotation (annotations are sorted by length too):
                # marker will not be part of any annotation
                if idx not in inserts:
                    inserts[idx].append((start_marker, []))
                else:
                    inserts[idx].append(
                        (start_marker, [i for i in at_beginning if i != a.id])
                    )
                    at_beginning.append(a.id)

            else:
                _, anns = chars[idx]
                inserts[idx].append((start_marker, [i for i in anns if i != a.id]))

            end = start + len(a.text)
            idx = end - 1
            _, anns = chars[idx]
            inserts[idx].append((end_marker, [i for i in anns if i != a.id]))

        return inserts

    def apply_inserts(
        self, chars: list[tuple[str, list]], inserts: dict[int, list]
    ) -> list[tuple[str, list]]:
        """
        Apply insertion ops (mention markers) to list of characters
        """

        chars_with_inserts = []

        # nested annotation has same start of parent
        if -1 in inserts:
            # multiple nested annotations can have same start
            for (ichars, ianns) in inserts[-1]:
                for ichar in ichars:
                    chars_with_inserts.append((ichar, ianns))

        for idx, (c, anns) in enumerate(chars):
            chars_with_inserts.append((c, anns))
            # multiple nested annotations can have same start
            if idx in inserts:
                for (ichars, ianns) in inserts[idx]:
                    for ichar in ichars:
                        chars_with_inserts.append((ichar, ianns))

        return chars_with_inserts

    def add_markers_to_grouped_annotations(
        self, annotations: list[Annotation], passage: Passage
    ) -> tuple[str, int]:
        """
        Add markers to grouped annotations (nested, overlapping).
        """

        chars, span_offset = self.get_text_span_multi_annotation(
            passage=passage, annotations=annotations
        )

        inserts = self.get_grouped_annotations_inserts(
            annotations=annotations,
            chars=chars,
            span_offset=span_offset,
            passage_offset=passage.offset,
        )

        chars_with_inserts = self.apply_inserts(chars=chars, inserts=inserts)

        for a in annotations:
            a.text = "".join(
                [char for char, anns in chars_with_inserts if a.id in anns]
            )

        span_text = "".join([char for char, _ in chars_with_inserts])

        # get # of characters added for sanity check
        chars_added = 0
        for inserts_list in inserts.values():
            for (ichars, _) in inserts_list:
                chars_added += len(ichars)

        return span_text, chars_added

    def add_markers_to_passage(self, eid: str, passage: Passage) -> Passage:
        """
        Add markers before and ater annotation in text.
        """

        text = ""
        chunk_end = 0
        chars_added = 0

        grouped_annotations = self.group_annotations_by_span(passage.annotations)

        for ga in grouped_annotations:

            is_independent_annotation = self.is_independent_annotation(ga)

            chunk_start = (
                ga[0].start - passage.offset
                if is_independent_annotation
                else min(a.start for a in ga) - passage.offset
            )

            text += passage.text[chunk_end:chunk_start]

            chunk_end = (
                ga[0].end - passage.offset
                if is_independent_annotation
                else max(a.end for a in ga) - passage.offset
            )

            if is_independent_annotation:
                a = ga[0]
                start_marker, end_marker = self.get_markers(entity_type=a.entity_type)
                # add in-text start marker
                text += start_marker
                chars_added += len(start_marker)
                # insert annotation text
                a.start = len(text)
                text += a.text
                a.end = a.start + len(a.text)
                # add in-text end marker
                text += end_marker
                chars_added += len(end_marker)
            else:
                text_chunk, tmp_chars_added = self.add_markers_to_grouped_annotations(
                    annotations=ga, passage=passage
                )
                self.remap_offsets_grouped_annotations(
                    annotations=ga,
                    span_text=text_chunk,
                    offset=len(text),
                )
                text += text_chunk
                chars_added += tmp_chars_added

        # add missing text
        text += passage.text[chunk_end:]

        if len(text) != len(passage.text) + chars_added:
            raise RuntimeError(
                f"EID:{eid} | add markers: length text {len(text)} != original+markers {len(passage.text)+chars_added}!"
            )

        passage.text = text

        return passage

    @transformmethod
    def apply(self, example: Example) -> Example:

        passages = []

        for p in example.passages:

            if len(p.annotations) > 0:

                p = self.add_markers_to_passage(eid=example.id, passage=p)

            passages.append(p)

        example.passages = passages
        example.passages_text_modified = True
        example.annotations_offsets_relative_to_passage = True

        return example

    @transformmethod
    def safe_apply(self, example: Example) -> Example:
        """
        Apply w/ sanity checks
        """

        try:
            example = self.apply(example=example)
        except RuntimeError:
            example = self.handle_error(
                eid=example.id, msg=f"EID:{example.id} - Failed adding mention markers!"
            )

        if not example.is_empty:
            try:
                qaqc.test_offsets(example)
            except qaqc.OffsetError:
                example = self.handle_error(
                    eid=example.id,
                    msg=f"EID:{example.id} - Offsets are wrong after adding mention markers!",
                )

        return example
