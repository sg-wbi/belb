from pathlib import Path

import bioc
from bioc import pubtator

from ..utils import NIL


def parse_pubtator_example(example: pubtator.PubTator) -> dict:
    features = {
        "id": example.pmid,
        "text": example.text,
        "annotations": [],
    }

    for a in example.annotations:
        features["annotations"].append(
            {
                "type": a.type,
                "start": a.start,
                "end": a.end,
                "text": a.text,
                "ids": a.id.split(","),
            }
        )

    features["metadata"] = {
        "pmid": example.pmid,
        "passages": [
            {"offset": 0, "type": "title"},
            {"offset": len(example.title), "type": "abstract"},
        ],
    }

    return features


def convert_bioc_to_pubtator(
    example: bioc.BioCDocument, identifier_key: str, type_key: str = "type"
) -> pubtator.PubTator:
    if len(example.passages) > 2:
        raise ValueError(
            f"Cannot convert {type(example)} to PubTator: more than two passages found (must be only title and abstract)"
        )

    title = None
    abstract = None
    annotations = []
    for p in example.passages:
        passage_type = p.infons.get("type", "")

        if passage_type.lower() == "title":
            title = p.text

        elif passage_type.lower() == "abstract":
            abstract = p.text

        for a in p.annotations:
            annotations.append(
                pubtator.PubTatorAnn(
                    pmid=example.id,
                    start=a.total_span.offset,
                    end=a.total_span.end,
                    text=a.text,
                    id=a.infons.get(identifier_key),
                    type=a.infons["type"],
                )
            )

    if title is None and abstract is None:
        raise RuntimeError("Could not determine passage types")

    doc = pubtator.PubTator(pmid=example.id, title=title, abstract=abstract)
    doc.annotations = annotations

    return doc


def parse_brat_annotation(line: str, pmid: str = "") -> list[pubtator.PubTatorAnn]:
    fields = line.split("\t")

    ann_id = fields[0]  # "T7"
    type_and_spans = fields[1]  # "Species 650 658;680 687"
    full_text = fields[2]  # "Gouldian finches"

    ann_type, span_str = type_and_spans.split(" ", 1)  # "Species", "650 658;680 687"
    spans = []
    for span in span_str.split(";"):
        start_str, end_str = span.split()
        spans.append((int(start_str), int(end_str)))

    # Split discontiguous text into chunks (mirrors the brat parsing logic)
    texts = []
    if len(spans) > 1:
        i = 0
        for start, end in spans:
            chunk_len = end - start
            texts.append(full_text[i : chunk_len + i])
            i += chunk_len
            while i < len(full_text) and full_text[i] == " ":
                i += 1
    else:
        texts = [full_text]

    return [
        pubtator.PubTatorAnn(
            pmid=pmid,
            start=start,
            end=end,
            text=text,
            type=ann_type,
            id=ann_id,
        )
        for (start, end), text in zip(spans, texts)
    ]


def convert_brat_to_pubtator(
    ann_file: Path,
    txt_file: Path,
    pmid: str | None = None,
    offset: int | None = None,
    keep_kb_name: bool = False,
):

    if pmid is None:
        pmid = ann_file.stem

    annotations = []
    tid_to_id = {}
    for line in ann_file.read_text(encoding="utf-8").splitlines():
        if line.startswith("T"):
            annotations.extend(parse_brat_annotation(line=line, pmid=pmid))
        elif line.startswith("N"):
            fields = line.split("\t")[1].split()

            tid = fields[1]

            if tid in tid_to_id:
                raise ValueError(f"Multiple linking for {tid} in `{ann_file}`")

            tid_to_id[tid] = fields[2] if keep_kb_name else fields[2].split(":")[1]
    for a in annotations:
        a.id = tid_to_id.get(a.id, NIL)

    with txt_file.open() as fp:
        text = fp.read()

    if offset is None:
        offset = text.index("\n")

    example = pubtator.PubTator(
        pmid=pmid, title=text[:offset], abstract=text[offset + 1 :]
    )
    example.annotations = annotations
    return example
