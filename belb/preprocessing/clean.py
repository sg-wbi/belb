#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Interface to clean Intra Word Mentions
"""

from loguru import logger

from belb.preprocessing import qaqc
from belb.preprocessing.data import Annotation, Example, Passage
from belb.preprocessing.transform import BaseTransformation, transformmethod


def is_iwm_start(annotation: Annotation, passage: Passage) -> bool:
    """
    Mentions is directly attached to previous word
    """

    res = False

    start = annotation.start - passage.offset

    if start == 0:
        res = False
    else:
        res = passage.text[start - 1].isalnum()

    return res


def is_iwm_end(annotation: Annotation, passage: Passage) -> bool:
    """
    Mentions is directly attached to next word
    """

    res = False

    end = annotation.end - passage.offset

    if end == len(passage.text):
        res = False
    else:
        res = passage.text[end].isalnum()
    return res


def is_iwm(annotation: Annotation, passage: Passage) -> bool:
    """
    Check if annotation is an intra-word mention
    """

    res = is_iwm_start(annotation=annotation, passage=passage) or is_iwm_end(
        annotation=annotation, passage=passage
    )

    return res


def has_intra_word_mentions(p: Passage):
    """
    Chekc if passage has intra-word mentions
    """

    return any(is_iwm(a, p) for a in p.annotations)


class CleanIntraWordMentions(BaseTransformation):
    """
    Add whitespace to intra-word annotations
    """

    def get_grouped_annotations_inserts(
        self,
        annotations: list[Annotation],
        chars: list[tuple[str, list]],
        span_offset: int,
        passage_offset: int,
    ) -> tuple[dict, int]:
        """
        Determine where (after which character index) whitespaces must be included.
        """

        white_spaces_added = 0
        inserts = {}

        for a in annotations:

            # is_iwm_start
            start = a.start - passage_offset - span_offset
            if not start == 0:
                idx = start - 1
                char, anns = chars[idx]
                if char.isalnum():
                    # the space is not part of the annotation
                    inserts[idx] = (" ", [i for i in anns if i != a.id])
                    white_spaces_added += 1

            # is_iwm_end
            end = start + len(a.text)
            if not end == len(chars):
                idx = end - 1
                char, anns = chars[idx]
                if char.isalnum():
                    # the space is not part of the annotation
                    inserts[idx] = (" ", [i for i in anns if i != a.id])
                    white_spaces_added += 1

        return inserts, white_spaces_added

    def apply_inserts(
        self, chars: list[tuple[str, list]], inserts: dict
    ) -> list[tuple[str, list]]:
        """
        Apply insertion ops (white spaces) to list of characters
        """
        # get characters with necessary whitespaces
        chars_with_inserts = []
        for idx, (c, anns) in enumerate(chars):
            chars_with_inserts.append((c, anns))
            if idx in inserts:
                chars_with_inserts.append(inserts[idx])

        return chars_with_inserts

    def clean_grouped_annotations_intraword_mention(
        self, annotations: list[Annotation], passage: Passage
    ) -> tuple[str, int]:
        """
        Add white spaces in annotations having nested annotations
        """

        chars, span_offset = self.get_text_span_multi_annotation(
            passage=passage, annotations=annotations
        )

        inserts, white_spaces_added = self.get_grouped_annotations_inserts(
            annotations=annotations,
            chars=chars,
            span_offset=span_offset,
            passage_offset=passage.offset,
        )

        chars_with_inserts = self.apply_inserts(chars=chars, inserts=inserts)

        # rebuild annotation text from characters
        for a in annotations:
            a.text = "".join([c for c, anns in chars_with_inserts if a.id in anns])

        span_text = "".join([char for char, _ in chars_with_inserts])

        return span_text, white_spaces_added

    def clean_passage_intra_word_mentions(self, eid: str, passage: Passage) -> Passage:
        """
        Add white space to all intra-word mentions
        """

        chunk_end = 0
        text = ""
        white_spaces_added = 0

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
                prepend_white_space = is_iwm_start(annotation=a, passage=passage)
                append_white_space = is_iwm_end(annotation=a, passage=passage)

            else:
                prepend_white_space = is_iwm_start(
                    annotation=min(ga, key=lambda x: x.start),
                    passage=passage,
                )
                append_white_space = is_iwm_end(
                    annotation=max(ga, key=lambda x: x.end),
                    passage=passage,
                )

            if prepend_white_space:
                text += " "
                white_spaces_added += 1

            if is_independent_annotation:
                a.start = len(text)
                a.end = a.start + len(a.text)
                text += a.text
            else:
                (
                    text_chunk,
                    tmp_white_spaces_added,
                ) = self.clean_grouped_annotations_intraword_mention(
                    annotations=ga, passage=passage
                )

                self.remap_offsets_grouped_annotations(
                    annotations=ga,
                    span_text=text_chunk,
                    offset=len(text),
                )
                text += text_chunk
                white_spaces_added += tmp_white_spaces_added

            if append_white_space:
                text += " "
                white_spaces_added += 1

        # add missing text
        text += passage.text[chunk_end:]

        if len(text) != len(passage.text) + white_spaces_added:
            raise RuntimeError(
                f"EID:{eid} | iwm: length text {len(text)} != original+white spaces {len(passage.text)+white_spaces_added}!"
            )

        passage.text = text

        return passage

    @transformmethod
    def apply(self, example: Example) -> Example:
        """Clean intra-word mentions in passages"""

        passages = []

        for p in example.passages:

            if has_intra_word_mentions(p):

                p = self.clean_passage_intra_word_mentions(eid=example.id, passage=p)

            else:

                # NOTE: make annotations relative to passage
                # hack for pipeline: instead of having workaround
                # and go crazy w/ figuring out offsets
                for a in p.annotations:
                    a.start = a.start - p.offset
                    a.end = a.start + len(a.text)

            passages.append(p)

        example.passages = passages
        example.passages_text_modified = True
        example.annotations_offsets_relative_to_passage = True

        return example

    @transformmethod
    def safe_apply(self, example: Example) -> Example:

        try:
            qaqc.test_intraword_mentions(example=example)
        except qaqc.IntraWordMentionError:
            try:
                example = self.apply(example=example)
            except RuntimeError as error:
                example = self.handle_error(
                    eid=example.id,
                    msg=f"EID:{example.id} - Cleaning intra-word mentions failed with error: \n {error}",
                )

            if not example.is_empty:
                logger.debug(
                    "EID:{} - Cleaned text with intra-word mention(s)", example.id
                )
                try:
                    qaqc.test_offsets(example)
                except qaqc.OffsetError:
                    example = self.handle_error(
                        eid=example.id,
                        msg=f"EID:{example.id} - Wrong offsets after cleaining intra-word mentions!",
                    )

            if not example.is_empty:
                try:
                    qaqc.test_intraword_mentions(example=example)
                except qaqc.IntraWordMentionError:
                    example = self.handle_error(
                        eid=example.id,
                        msg=f"EID:{example.id} - Intra-word mentions are still present after cleaining!",
                    )
        return example
