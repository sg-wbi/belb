from collections.abc import Mapping

import pandas as pd
from datasets import DatasetDict
from tqdm import tqdm


def qaqc_offsets(corpus: DatasetDict) -> pd.DataFrame:

    rows = []
    ds_name = None
    for split_name in corpus:

        ds = corpus[split_name]
        if ds_name is None:
            ds_name = ds.info.dataset_name

        for e in ds:

            text = e["text"]

            for a in e["annotations"]:

                start, end, ann_text = a["start"], a["end"], a["text"]

                char_text = text[start:end]

                if char_text != ann_text:
                    row = {
                        "id": e["id"],
                        "start": start,
                        "end": end,
                        "text_expected": ann_text,
                        "text_by_offset": char_text,
                    }
                    rows.append(row)

    return pd.DataFrame(rows)


def qaqc_ids(corpus: DatasetDict, kb: DatasetDict) -> set[str]:

    if not all(isinstance(x, DatasetDict) for x in [corpus, kb]):
        raise TypeError("Both `corpus` and `kb` must be an instance of `DatasetDict`")

    if "entities" not in kb:
        raise KeyError("Expected `kb` to have key `entities`")

    entities = kb["entities"]

    if "id" not in entities.column_names:
        raise ValueError("`id` not found in `entities.column_names`")

    kb_ids: set = {i for i in tqdm(kb["entities"]["id"], desc="qaqc: collect KB ids")}

    try:
        corpus_ids = {
            i
            for split in tqdm(corpus.values(), desc="qaqc: collect corpus ids")
            for annotations in split["annotations"]
            for a in annotations
            for i in a["ids"]
        }
    except KeyError:
        raise KeyError(
            "Expcted each value in `corpus` to have key `annotations`",
            "and `annotations` value to be a list of items with key `ids`",
        )

    missing = {
        i
        for i in tqdm(corpus_ids, desc="qaqc: identify not-in-kb ids")
        if i not in kb_ids
    }

    return missing


def get_id_map(self, corpus: DatasetDict, kb: DatasetDict) -> Mapping[str, str] | None:

    missing = qaqc_ids(corpus=corpus, kb=kb)

    if len(missing) > 0:

        if "history" not in kb:
            raise KeyError("Expected `kb` to have key `history`")

        history = kb["history"]

        try:
            update = dict(zip(history["obsolete"], history["update"]))
        except KeyError:
            raise KeyError(
                "Expcted `history` value in `kb` to have keys  `obsolete` and `update`"
            )

        return {i: update[i] for i in missing if i in update}

    return None
