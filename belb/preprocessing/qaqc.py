#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Utilities for data quality: offsets, identifiers, intra-word mentions
"""

from collections import Counter

from loguru import logger

from belb.preprocessing.clean import is_iwm
from belb.preprocessing.data import NA, OBSOLETE_IDENTIFIER, Example


class OffsetError(Exception):
    """Wrong offset reported in annotation"""


class IdentifierError(Exception):
    """identifier reported in annotation not found in KB"""


class IntraWordMentionError(Exception):
    """Annotation is intra-word mention, e.g. [[IL-6]alpha]"""


def test_offsets(example: Example):
    """
    Verify that offsets are correct for all annotations
    """

    checks = []
    errors = []

    for p in example.passages:
        for a in sorted(p.annotations, key=lambda x: x.start):

            start = a.start - p.offset
            end = a.end - p.offset

            anntext_by_offset = p.text[start:end]

            if a.text == anntext_by_offset:
                checks.append(True)

            else:
                checks.append(False)
                errors.append(
                    f"\t `{anntext_by_offset}` (by offset [{a.start}, {a.end}]) != `{a.text}` (text)"
                )

    passed = all(checks)

    if not passed:
        raise OffsetError(
            f"\n EID:{example.id} (merge passages with white space): \n"
            + "\n".join(errors)
        )


def test_intraword_mentions(example: Example):
    """
    Test for IWM
    """

    checks = []
    errors = []

    text = " ".join([p.text for p in example.passages])

    for p in example.passages:

        for a in sorted(p.annotations, key=lambda x: x.start):

            if is_iwm(annotation=a, passage=p):
                checks.append(False)

                start = a.start - p.offset - 5 if a.start - p.offset - 5 > 0 else 0
                end = (
                    a.end - p.offset + 5
                    if a.end - p.offset + 5 <= len(p.text)
                    else len(p.text)
                )
                window_err_msg = text[start:end]
                errors.append(
                    f"\t `{a.text}` (offset: [{a.start}, {a.end}]) is intra-word mention: `{window_err_msg}`"
                )
            else:
                checks.append(True)

    passed = all(checks)

    if not passed:
        raise IntraWordMentionError(f"\n EID:{example.id}: \n" + "\n".join(errors))


def amend_identifiers(
    identifiers: list[str], notinkb_identifiers_history: dict
) -> tuple[list, Counter, list]:
    """
    Use `notinkb_identifiers_history` to fix list of identifiers.
    Generate log for each identifier not-in-kb.
    """

    amended_identifiers = []
    stats: Counter = Counter()
    logs: list = []

    for i in identifiers:
        if i not in notinkb_identifiers_history:
            amended_identifiers.append(i)
            continue
        history = notinkb_identifiers_history[i]
        # COUNT ONE PER ANNOTATION
        if history == NA:
            stats["ann_identifier_na"] = 1
            msg = f"\t removing identifier `{i}`: not in KB & no history"
        elif history == OBSOLETE_IDENTIFIER:
            stats["ann_identifier_discontinued"] = 1
            msg = f"\t removing identifier `{i}`: discontinued"
        else:
            stats["ann_identifier_replaced"] = 1
            msg = f"\t replacing identifier `{i}` with `{history}` (merged/new)"
            amended_identifiers.append(history)

        logs.append(msg)

    return amended_identifiers, stats, logs


def amend_annotations_identifiers(
    example: Example, notinkb_identifiers_history: dict
) -> Counter:
    """
    Amend identifiers in example annotations
    """

    example_logs = []
    example_stats: Counter = Counter()

    for p in example.passages:

        annotations_to_drop = []

        for _, a in enumerate(p.annotations):

            if a.foreign:
                continue

            assert a.identifiers is not None and isinstance(
                a.identifiers, list
            ), "Annotation was not processed!"

            amended_identifiers, stats, logs = amend_identifiers(
                identifiers=a.identifiers,
                notinkb_identifiers_history=notinkb_identifiers_history,
            )

            example_stats.update(stats)
            example_logs += logs

            a.identifiers = amended_identifiers

            if len(a.identifiers) == 0:
                annotations_to_drop.append(a)
                original_identifiers = a.original["identifiers"]
                msg = "\t removing annotation w/o identifiers after sanity check."
                msg += f" Original identifiers: `{original_identifiers}`"
                example_logs.append(msg)

        p.annotations = [a for a in p.annotations if a not in annotations_to_drop]

    if len(example_logs) > 0:
        logger.debug(
            "\n EID:{} - identifiers sanity check: \n" + "\n".join(example_logs),
            example.id,
        )

    return example_stats
