#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Split into sentences and pair w/ respective annotations
"""

import re
from itertools import groupby
from operator import itemgetter

from syntok import segmenter

from belb.preprocessing import qaqc
from belb.preprocessing.data import Annotation, Example, Passage
from belb.preprocessing.transform import BaseTransformation, transformmethod


class SplitIntoSentences(BaseTransformation):
    """
    Transformation: split annotated text into sentences
    """

    def __init__(self, *args, sentinel_token: str = "@", **kwargs):
        super().__init__(*args, **kwargs)
        self.sentinel_token = sentinel_token

    def get_sentences_offsets(self, text: str) -> dict:
        """
        Get offsets of sentences
        """

        offsets: dict = {}
        idx = 0
        for paragraph in segmenter.analyze(text):
            for sent in paragraph:
                start = sent[0].offset
                end = sent[-1].offset + len(sent[-1].value)
                offsets[idx] = (start, end)
                idx += 1

        return offsets

    def find_sentences_to_be_merged(
        self, sentences_offsets: dict, passage: Passage
    ) -> list[tuple]:
        """
        Find pair of sentences to be merged
        """

        contiguous_sentences = set()
        for a in passage.annotations:
            for i in range(len(sentences_offsets) - 1):

                x_start, x_end = sentences_offsets[i]
                y_start, y_end = sentences_offsets[i + 1]

                x_range = range(x_start, x_end + 1)
                y_range = range(y_start, y_end + 1)

                if (
                    a.start - passage.offset in x_range
                    and a.end - passage.offset in y_range
                ):
                    contiguous_sentences.add(i)
                    contiguous_sentences.add(i + 1)

        merges = []
        for _, g in groupby(
            enumerate(sorted(contiguous_sentences)), key=lambda ix: ix[0] - ix[1]
        ):
            merges.append(tuple(map(itemgetter(1), g)))

        return merges

    def apply_merges_to_sentences_offsets(
        self, offsets: dict, merges: list[tuple]
    ) -> dict:
        """
        Change offsets list by replacing offsets of sentences to be merged (a pair)
        with a one offset: (start of first sentence, end of second sentence)
        """

        for m in merges:
            first_sent = m[0]
            last_sent = m[-1]
            start = offsets[first_sent][0]
            end = offsets[last_sent][1]
            offsets[first_sent] = (start, end)
            for idx in m[1:]:
                offsets.pop(idx)

        offsets = dict(enumerate(sorted(offsets.values(), key=lambda x: x[0])))

        return offsets

    def merge_sentences_with_crossing_annotations(
        self, sentences_offsets: dict, passage: Passage
    ) -> dict:
        """
        Merge sentences by pairs if there is an annotation spanning both of them (the sentences).
        """

        merges = self.find_sentences_to_be_merged(
            sentences_offsets=sentences_offsets, passage=passage
        )

        if len(merges) == 0:
            return sentences_offsets

        sentences_offsets = self.apply_merges_to_sentences_offsets(
            offsets=sentences_offsets, merges=merges
        )

        return self.merge_sentences_with_crossing_annotations(
            sentences_offsets=sentences_offsets, passage=passage
        )

    def get_annotations_grouped_by_sentence(
        self, sentences_offsets: dict, passage: Passage
    ) -> list[list[Annotation]]:
        """
        Group annotations by sentences offset
        """

        grouped_annotations = []

        for (start, end) in sorted(sentences_offsets.values(), key=lambda x: x[0]):
            sentence_annotations = [
                a
                for a in passage.annotations
                if a.start - passage.offset >= start and a.end - passage.offset <= end
            ]
            grouped_annotations.append(sentence_annotations)

        # sanity check
        unpacked_annotations = [a for sublist in grouped_annotations for a in sublist]

        if len(unpacked_annotations) != len(passage.annotations):
            raise RuntimeError(
                f"""Failed placing annotations in sentences:
                # grouped ({len(unpacked_annotations)}) != # original ({len(passage.annotations)})
                """
            )

        return grouped_annotations

    def get_text_with_masked_annotations(self, eid: str, passage: Passage) -> str:

        """
        Replace text of annotations in the example text with a string
        composed of sentinel tokens of same length as annotation text.
        """

        chars = list(passage.text)
        for a in passage.annotations:
            mask = list(self.sentinel_token * len(a.text))
            start = a.start - passage.offset
            end = a.end - passage.offset
            try:
                chars[start:end] = mask
            except ValueError as error:
                try:
                    end = start + len(a.text)
                    chars[start:end] = mask
                except ValueError:
                    raise RuntimeError(
                        f"EID:{eid} | Cannot mask annotation: `({a.text}, {a.start}, {a.end})`!"
                    ) from error

        masked_text = "".join(chars)

        if len(masked_text) != len(passage.text):
            raise RuntimeError(
                f"Failed masking annotations: masked text ({len(masked_text)}) != original ({len(passage.text)})"
            )

        return masked_text

    def replace_masked_annotations_and_remap_offsets(
        self, text: str, annotations: list[Annotation], passage: Passage
    ) -> tuple[str, list[Annotation]]:
        """
        Replace sentinel tokens in text w/ actual annotation text
        and remap the offsets of annotations to be relative to the sentence text.
        This is done to (try to) ensure that we are not introducing spurious
        annotations by using regexps to compute the offsets in the sentence
        """

        last_match = 0
        grouped_annotations = self.group_annotations_by_span(annotations)

        for ga in grouped_annotations:

            if self.is_independent_annotation(ga):
                a = ga[0]
                annotation_text = a.text
            else:
                chars, _ = self.get_text_span_multi_annotation(
                    passage=passage, annotations=ga
                )
                annotation_text = "".join(c for c, ann_ids in chars)

            length = len(annotation_text)
            pattern = re.compile(f"{re.escape(self.sentinel_token)}{{{length}}}")
            match = re.search(pattern, text[last_match:])

            if match is None:
                raise RuntimeError(
                    f"Could not remap annotation(s) `{[a.text for a in ga]}` in sentence `{text}`"
                )

            # replace sentinel_token with original text
            sentinel_chars = self.sentinel_token * length
            text = re.sub(sentinel_chars, annotation_text, text, 1)

            # add sentence text so far to offset
            offset = len(text[:last_match])

            # for next search, start looking after end of this annotation
            last_match = match.end() + offset

            if self.is_independent_annotation(ga):
                a = ga[0]
                a.start = match.start() + offset
                a.end = a.start + len(a.text)
            else:
                self.remap_offsets_grouped_annotations(
                    annotations=ga,
                    span_text=annotation_text,
                    offset=match.start() + offset,
                )

        return text, annotations

    def replace_sentinel_token(self, passage: Passage) -> Passage:
        """
        Substitute `sentinel_token` w/ whitespace in text (passage and annotations)
        otherwise mask replacement breaks.
        """

        # get rid of pre-existing sentinel tokens
        passage.text = passage.text.replace(self.sentinel_token, " ")

        for a in passage.annotations:
            a.text = a.text.replace(self.sentinel_token, " ")

        return passage

    @transformmethod
    def apply(self, example: Example):

        sentences = []

        for p in example.passages:

            p = self.replace_sentinel_token(p)

            # compute sentences offsets (relative to passage, i.e. starting from 0)
            sentences_offsets = self.get_sentences_offsets(p.text)

            # merge offsets if there is annotation crossing sentences boundaries
            sentences_offsets = self.merge_sentences_with_crossing_annotations(
                sentences_offsets=sentences_offsets, passage=p
            )

            # group annotations by sentence
            bysent_annotations = self.get_annotations_grouped_by_sentence(
                sentences_offsets=sentences_offsets, passage=p
            )

            # mask annotation text in sentence with sentinel token (e.g. `@`)
            masked_text = self.get_text_with_masked_annotations(
                eid=example.id, passage=p
            )

            for (start, end), annotations in zip(
                sorted(sentences_offsets.values(), key=lambda x: x[0]),
                bysent_annotations,
            ):
                text = masked_text[start:end]
                if len(annotations) > 0:
                    # go through annotation one by one
                    # and try to match # of sentinel tokens with # of characters in annotation
                    # avoid introducing spurious annotations by using regexp to compute offsets
                    (
                        text,
                        annotations,
                    ) = self.replace_masked_annotations_and_remap_offsets(
                        passage=p, text=text, annotations=annotations
                    )

                sentence = Passage(
                    id=p.id,
                    offset=start,
                    text=text,
                    annotations=annotations,
                    type=p.type,
                )

                sentences.append(sentence)

        example.passages = sentences
        example.passages_text_modified = True
        example.annotations_offsets_relative_to_passage = True

        return example

    @transformmethod
    def safe_apply(self, example: Example):
        """
        Try to split into sentences example's text.
        Each sentence is a passage with its own annotations.
        After splitting, check offsets: return empty example if `allow_drop=True` else raise error.
        """
        try:
            example = self.apply(example=example)
        except RuntimeError as error:
            example = self.handle_error(
                eid=example.id,
                msg=f"EID:{example.id} - Sentence splitting failed with error: \n {error}",
            )

        if not example.is_empty:
            try:
                qaqc.test_offsets(example)
            except qaqc.OffsetError:
                example = self.handle_error(
                    eid=example.id,
                    msg=f"EID:{example.id} - After splitting into sentences example throws OffsetError",
                )
        return example
