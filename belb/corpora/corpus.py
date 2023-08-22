#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Define base structure for entity linking corpus
"""
import argparse
import copy
import json
import os
import random
from collections import Counter
from typing import Optional

import bioc
import pandas as pd
from bioc import biocjson
from loguru import logger

from belb.corpora.parser import (BaseBelbCorpusConfig, BaseBelbCorpusParser,
                                 Splits)
from belb.kbs.kb import BelbKb
from belb.preprocessing import qaqc
from belb.preprocessing.clean import CleanIntraWordMentions
from belb.preprocessing.data import (INVALID_POSITION, NA, Annotation, Example,
                                     Passage)
from belb.preprocessing.mark import AddMentionMarkers
from belb.preprocessing.segment import SplitIntoSentences
from belb.utils import (METADATA, chunkize, load_json,
                        load_manual_notinkb_history, save_json)
from belb.utils.pubtator_central import PubTatorDB


class CorpusConverter:
    """
    Base class to convert corpus in BELB format
    """

    def __init__(
        self,
        directory: str,
        parser: BaseBelbCorpusParser,
        kb: BelbKb,
        pubtator: Optional[str] = None,
    ):

        self.kb = kb
        self.parser = parser
        self.config = self.parser.config
        self.pubtator = pubtator
        if self.config.pmc and self.pubtator is None:
            raise ValueError(
                "If the corpus is full text (`pmc=True`) `pubtator` cannot be None!"
            )

        self.root_directory = directory
        self.download_directory = os.path.join(
            self.root_directory, "raw", "corpora", self.config.name
        )
        os.makedirs(self.download_directory, exist_ok=True)
        self.base_directory = os.path.join(
            self.root_directory,
            "processed",
            "corpora",
            self.config.name,
        )
        self.processed_directory = os.path.join(
            self.base_directory, self.config.to_hexdigest()
        )
        os.makedirs(self.processed_directory, exist_ok=True)
        assert self.config.entity_types is not None
        self.name: str = (
            f"{self.config.name}-{self.config.entity_type}"
            if len(self.config.entity_types) > 1
            else self.config.name
        )
        self.config.save(self.processed_directory)

        self.data: dict = {}
        self.manual_notinkb_identifiers_history: dict = {}
        self.notinkb_identifiers_history: dict = {}
        self.foreign_annotations: dict = {}
        self.annotations: Optional[dict] = None
        if self.config.annotations_file is not None:
            self.annotations = load_json(self.config.annotations_file)

        self.transformations: dict = {"iwm": CleanIntraWordMentions()}
        if self.config.sentences:
            self.transformations["sentences"] = SplitIntoSentences()
        if self.config.mention_markers:
            self.transformations["markers"] = AddMentionMarkers(
                entity_type=self.config.entity_type
            )

        self.qaqc_stats: dict = {}

    @property
    def cache_dir(self) -> str:
        """
        Store here stuff fetched from KB
        """

        cache_dir = [self.base_directory]

        assert self.config.entity_types is not None
        if len(self.config.entity_types) > 1:
            cache_dir.append(self.config.entity_type)

        if self.config.subsets is not None:
            assert self.config.subset is not None
            cache_dir.append(self.config.subset)

        path = os.path.join(*cache_dir)
        os.makedirs(path, exist_ok=True)

        return path

    @staticmethod
    def get_argument_parser(description: str) -> argparse.ArgumentParser:
        """
        Argument parser for supervised corpora

        Parameters
        ----------
        name : str
            Name of the corpus
        """

        parser = argparse.ArgumentParser(description=description)
        parser.add_argument(
            "--dir", required=True, type=str, help="Directory where BELB data is stored"
        )
        parser.add_argument(
            "--db",
            required=True,
            type=str,
            help="Database configuration",
        )
        parser.add_argument(
            "--pubtator",
            type=str,
            default=None,
            help="Path to local PubTator sqlite DB (built from `bioconcepts2pubtator.offsets`)",
        )
        parser.add_argument(
            "--sentences", action="store_true", help="Split texts into sentences"
        )
        parser.add_argument(
            "--markers", action="store_true", help="Mark mentions in text"
        )
        parser.add_argument(
            "--exclude_foreign",
            action="store_true",
            help="Do not add helper annotations even if supported",
        )
        parser.add_argument(
            "--annotations_file",
            type=str,
            default=None,
            help="Path to file containing corpus annotations (hexdigests)",
        )
        parser.add_argument(
            "--max_mentions",
            type=int,
            default=-1,
            help="Upsample each example to have this max amount of mentions",
        )
        parser.add_argument("--debug", action="store_true", help="Log level to DEBUG")

        return parser

    def write_collection(self, path: str, documents: list[bioc.BioCDocument]):
        """
        Write collection of BioC documents as JSON.
        """

        collection = bioc.BioCCollection()
        collection.documents = documents

        with open(path, "w", encoding="utf-8") as fp:
            biocjson.dump(collection, fp, indent=1, ensure_ascii=False)

    def load_data(self, preprocess: bool = True):
        """
        Load all examples
        """

        if (METADATA / self.name).is_dir():
            # id mapping is entity_type-specific
            self.manual_notinkb_identifiers_history.update(
                load_manual_notinkb_history(self.name)
            )

        assert self.config.splits is not None
        for split in self.config.splits:

            self.data[split] = []
            self.qaqc_stats[split] = Counter()

            for e in self.parser.load_split(
                directory=self.download_directory, split=split
            ):

                if self.annotations is not None:

                    e.filter_annotations(hexdigests=set(self.annotations[split]))

                    if e.is_empty:
                        logger.warning(
                            "EID:{} - Example is empty after filtering with annotations hexdigests..."
                        )
                        continue

                if preprocess:
                    e, qaqc_stats = self.preprocess_example(example=e)
                    self.qaqc_stats[split].update(qaqc_stats)

                self.data[split].append(e)

    def load_notinkb_identifiers_history(self, batch_size: Optional[int] = None):
        """
        Load (if cached otherwise fetch from KB) history of identifiers not-in-kb.
        """

        path = os.path.join(self.cache_dir, "notinkb_identifiers_history.json")

        if not os.path.exists(path):

            logger.info("Fetching history of not-in-kb identifiers...")

            with self.kb as handle:
                handle.save_notinkb_history(
                    path=path,
                    identifiers=self.get_corpus_identifiers(),
                    chunksize=batch_size,
                )

        self.notinkb_identifiers_history.update(
            {str(k): v for k, v in load_json(path).items()}
        )

    @staticmethod
    def extract_config_options_from_args(args: argparse.Namespace) -> dict:
        """
        Build configuration from CLI args
        """

        config: dict = {
            "sentences": args.sentences,
            "mention_markers": args.markers,
            "annotations_file": args.annotations_file,
            "max_mentions" : args.max_mentions
        }

        if args.exclude_foreign:
            config["add_foreign_annotations"] = False

        return config

    def set_annotation_entity_type(self, a: Annotation):
        """
        Assign a valid interal entity type. See `belb.preprocessing.datas.ENTITY_TYPES`.
        Most of the cases will be assigning corpus.config.entity_type
        """

        assert self.config.entity_types is not None
        if len(self.config.entity_types) > 1:
            assert self.config.entity_type_map is not None
            assert (
                self.config.entity_type_map is not None
            ), "`ENTITY_TYPE_MAP` is not defined!"
            a.entity_type = self.config.entity_type_map[a.entity_type]
        else:
            a.entity_type = self.config.entity_type

    def preprocess_example(self, example: Example) -> tuple[Example, dict]:
        """Example preprocessing: handling annotations"""

        qaqc_stats: Counter = Counter()

        for passage in example.passages:

            annotations, passage_qaqc_stats = self.preproccess_annotations(
                eid=example.id, p=passage
            )

            qaqc_stats.update(passage_qaqc_stats)

            passage.annotations = annotations

        return example, qaqc_stats

    def apply_manual_notinkb_identifiers_history(
        self, identifiers: list
    ) -> tuple[list, int]:
        """
        Apply corpus-specific mapping of identifiers
        """

        # COUNT ONE PER ANNOTATION
        num_mapped = 0

        if len(self.manual_notinkb_identifiers_history) > 0:
            for idx, i in enumerate(identifiers):
                if i in self.manual_notinkb_identifiers_history:
                    i = self.manual_notinkb_identifiers_history[i]
                    num_mapped = 1
                identifiers[idx] = i

        return identifiers, num_mapped

    def preproccess_annotations(
        self, eid: str, p: Passage
    ) -> tuple[list[Annotation], dict]:
        """
        Preprocess annotations in a given passage:
        - offsets
        - text
        - identifiers
        """

        annotations: list = []

        qaqc_stats: Counter = Counter()

        for a in p.annotations:

            if (a.start, a.end) == (INVALID_POSITION, INVALID_POSITION):
                qaqc_stats["offsets"] += 1
                continue

            if self.parser.handle_errors_annotation_offsets(eid=eid, a=a, p=p):
                qaqc_stats["offsets"] += 1

            if self.parser.handle_errors_annotation_text(eid=eid, a=a, p=p):
                qaqc_stats["text"] += 1

            self.set_annotation_entity_type(a)

            assert self.config.entity_types is not None
            assert (
                a.entity_type in self.config.entity_types
            ), f"EID:{eid} | Entity type must be one of {self.config.entity_types}. Found {a.entity_type}!"

            if not a.foreign:

                assert (
                    a.original.get("identifiers") is not None
                ), f"EID:{eid} | Annotation has no identifiers to process: {a}"

                identifiers = self.parser.parse_annotation_identifiers(
                    a.original["identifiers"]
                )

                qaqc_stats["identifiers_na_pre_kb"] += len(
                    [i for i in identifiers if i == NA]
                )

                identifiers = [i for i in identifiers if i != NA]

                identifiers, num_mapped = self.apply_manual_notinkb_identifiers_history(
                    identifiers=identifiers
                )

                qaqc_stats["ann_identifier_replaced_manual_mapping"] += num_mapped

                if len(identifiers) == 0:

                    logger.debug(
                        "EID:{} - Discarding annotation w/o valid identifiers ({})",
                        eid,
                        f"`{a.text}`: `{a.identifiers}`",
                    )
                    continue

                a.identifiers = identifiers

            annotations.append(a)

        return annotations, qaqc_stats

    def upsample(self, example: Example, max_mentions: int = 20) -> list[Example]:
        """
        Split into `n` examples with ~`max_mentions` mentions each

        NOTE: 20 seems to be a magical number in `faiss`
        """

        ids = [a.id for p in example.passages for a in p.annotations]

        if len(ids) > max_mentions:

            random.shuffle(ids)

            examples = []

            chunks = [ids[i:i+max_mentions] for i in range(0,len(ids), max_mentions)]

            for chunk in chunks:

                e = copy.deepcopy(example)

                for p in e.passages:
                    p.annotations = [a for a in p.annotations if a.id in chunk]

                examples.append(e)
        else:

            examples = [example]

        return examples

    def get_parsed_example(self, example: Example) -> tuple[list[Example], dict]:
        """
        Apply preprocessing to example.

        Sanity check:
            1. identifiers : `id` of mention must be in associated KB
            2. mentions offsets: mention `in text` == `text` associated to annotation
            3. intra-word mentions: insert white space in annotation e.g. `[IL-6]alpha` (breaks WordPiece tokenization)
        """

        qaqc_stats = qaqc.amend_annotations_identifiers(
            example=example,
            notinkb_identifiers_history=self.notinkb_identifiers_history,
        )

        if (
            self.config.add_foreign_annotations
            and example.id in self.foreign_annotations
        ):
            example.inject_foreign_annotations(self.foreign_annotations[example.id])

        example.prepare()

        qaqc.test_offsets(example=example)

        example = self.transformations["iwm"].safe_apply(example)

        examples = self.upsample(example=example, max_mentions=self.config.max_mentions) if self.config.max_mentions > 0 else [example]

        if self.config.sentences:
            examples = [
                self.transformations["sentences"].safe_apply(e) for e in examples
            ]

        if self.config.mention_markers:
            examples = [self.transformations["markers"].safe_apply(e) for e in examples]

        return examples, qaqc_stats

    def get_corpus_identifiers(self) -> set[str]:
        """
        Load all identifiers available in corpus
        """

        identifiers = set()

        for _, examples in self.data.items():
            for e in examples:
                for p in e.passages:
                    for a in p.annotations:
                        if a.foreign or a.identifiers is None:
                            continue
                        identifiers.update([str(i) for i in a.identifiers])

        if len(identifiers) == 0:
            raise RuntimeError(
                f"The corpus {self.config.name} has no identifiers! Parsing of the examples/annotations probably failed..."
            )

        return identifiers

    def save_foreign_annotations(self, path: str) -> dict:
        """
        Fetch annotations from PubTatorDB
        """

        assert (
            self.pubtator is not None
        ), "You need to specify the path to a local PubTator DB to add helper annotations"

        assert (
            self.config.foreign_entity_types is not None
        ), "You need to specify the type of helper annotations"

        ids = set(e.id for split, examples in self.data.items() for e in examples)

        logger.info(
            "{}: Fetching foreign annotations of type: {}...",
            self.name,
            tuple(str(t) for t in self.config.foreign_entity_types),
        )

        results = []

        with PubTatorDB(
            self.pubtator, entity_types=self.config.foreign_entity_types
        ) as db:

            for batch in chunkize(ids, chunksize=10000):

                results.append(db.fetch_annotations(batch))

        foreign_annotations = PubTatorDB.group_annotations_by_pmid(pd.concat(results))

        save_json(
            path=path,
            item=dict(foreign_annotations),
            kwargs={"json": {"indent": 1}},
        )

        logger.info(
            "{}: Completed fetching helper annotations...",
            self.name,
        )

        return foreign_annotations

    def load_foreign_annotations(self):
        """
        Fetch all PubTator annotations for all additional `entity_type`s.
        """

        path = os.path.join(self.cache_dir, "foreign_annotations.json")

        if not os.path.exists(path):

            self.save_foreign_annotations(path=path)

        self.foreign_annotations = {
            pmid: [
                Annotation(
                    start=a["start"],
                    end=a["end"],
                    text=a["text"],
                    entity_type=a["type"].lower()
                    if not a["type"] == "CellLine"
                    else "cell_line",
                    foreign=True,
                )
                for a in annotations
            ]
            for pmid, annotations in load_json(path).items()
        }

    def create_split(
        self,
        examples: list[Example],
        split: str,
    ) -> tuple[set, set]:
        """
        Create corpus split.

        Returns
        -------
        set
            example ids of split.
        """

        # logger.debug("{}: Start processing `{}` split", self.name, split)

        documents = []

        ids: set = set()
        hexdigests: set = set()

        for example in examples:

            parsed_examples, qaqc_stats = self.get_parsed_example(example=example)

            self.qaqc_stats[split].update(qaqc_stats)

            for pe in parsed_examples:

                if pe.is_empty:
                    logger.warning(
                        "EID:{} - Example is empty after preprocessing", pe.id
                    )
                    continue

                documents.append(pe.to_belb())

                ids.add(pe.id)
                hexdigests.update(pe.get_annotations_hexdigests())

        self.write_collection(
            path=os.path.join(self.processed_directory, f"{split}.bioc.json"),
            documents=documents,
        )

        logger.info(
            "{}: Created `{}` split with {} examples", self.name, split, len(documents)
        )

        return ids, hexdigests

    def to_belb(self):
        """
        Convert corpus
        """

        logger.info("*******")
        logger.info("Converting `{}` into BELB format", self.config.name)
        logger.info("*******")
        logger.info("Sentences: {}", self.config.sentences)
        logger.info("Markers: {}", self.config.mention_markers)
        if len(self.config.entity_types) > 1:
            logger.info("Entity type: {}", self.config.entity_type)
        if self.config.subsets is not None:
            logger.info("Subset: {}", self.config.subset)
        if self.config.foreign_entity_types is not None:
            logger.info(
                "Foreign annotations: {}", bool(len(self.config.foreign_entity_types))
            )
        if self.config.max_mentions is not None and self.config.max_mentions > -1:
            logger.info(
                "Max mentions per document (upsampling): {}", self.config.max_mentions
            )

        if not self.config.local:
            self.config.resource.download(directory=self.download_directory)

        self.parser.on_before_load(directory=self.download_directory, kb=self.kb)

        self.load_data()

        self.parser.on_after_load(data=self.data, kb=self.kb)

        self.load_notinkb_identifiers_history()

        if (
            self.config.add_foreign_annotations
            and not self.config.native_foreign_annotations
        ):
            self.load_foreign_annotations()

        corpus_ids = set()
        corpus_annotations = {}

        for split in self.config.splits:
            ids, annotations_hexdigests = self.create_split(
                examples=self.data[split], split=split
            )
            corpus_ids.update(ids)
            corpus_annotations[split] = annotations_hexdigests

        save_json(
            path=os.path.join(self.processed_directory, "annotations.json"),
            item={k: list(v) for k, v in corpus_annotations.items()},
            kwargs={"json": {"indent": 1}},
        )

        self.save_pmids(corpus_ids=corpus_ids)

        stats_path = os.path.join(self.cache_dir, "qaqc_stats.json")
        if not os.path.exists(stats_path):
            save_json(
                path=stats_path,
                item=dict(self.qaqc_stats),
                kwargs={"json": {"indent": 1}},
            )

        self.add_to_cached()

        logger.info("{}: Completed creating corpus", self.name)

    def add_to_cached(self):
        """
        Cache config hexdigest
        """
        path = os.path.join(
            self.root_directory, "processed", "corpora", "corpora.jsonl"
        )

        item = self.config.to_dict()
        item["hexdigest"] = self.config.to_hexdigest()

        write = True
        if os.path.exists(path):
            df = pd.read_json(path, lines=True)
            write = item["hexdigest"] not in set(df["hexdigest"])

        if write:
            with open(path, "a") as fp:
                fp.write(f"{json.dumps(item)}\n")

    def convert_pmcids_to_pmids(self, pmcids: set[str]):
        """
        pmcids must be only ID, i.e. no "PMC" prefix!
        """

        pmcid2pmid_path = os.path.join(self.processed_directory, "pmcid2pmid.json")

        if not os.path.exists(pmcid2pmid_path):

            logger.debug("{}: Fetching `PMCID -> PMID` mapping...", self.name)

            assert self.pubtator is not None, "No path to `PubTatorDB` available!"

            with PubTatorDB(self.pubtator) as db:
                pmcid2pmid = db.fetch_pmcid_to_pmid(pmcids)

            save_json(
                item=pmcid2pmid, path=pmcid2pmid_path, kwargs={"json": {"indent": 1}}
            )

        else:
            logger.info("Loading cached `PMCID -> PMID`...")
            pmcid2pmid = load_json(pmcid2pmid_path)

        example_ids = [pmcid2pmid[pmcid] for pmcid in pmcids if pmcid in pmcid2pmid]

        return example_ids

    def save_pmids(self, corpus_ids: set):
        """
        Convert PMC to PMID
        """

        if self.config.pmc:
            pmids = self.convert_pmcids_to_pmids(pmcids=corpus_ids)
        else:
            pmids = corpus_ids

        outfile_path = os.path.join(self.processed_directory, "pmids.txt")
        logger.debug("Saving PMIDs into `{}`", outfile_path)
        with open(outfile_path, "w") as outfile:
            for pmid in pmids:
                outfile.write(f"{pmid}\n")

class BelbCorpus:
    """
    Wrapper to access processed corpus
    """

    def __init__(self, directory: str, config: BaseBelbCorpusConfig):
        self.root_directory = directory
        self.config = config
        self.hexdigest = self.config.to_hexdigest()
        self.base_directory = os.path.join(
            self.root_directory,
            "processed",
            "corpora",
            self.config.name,
        )
        self.processed_directory = os.path.join(
            self.base_directory, self.hexdigest
        )

        assert os.path.exists(
            self.processed_directory
        ), f"Corpus was never converted to BELB with this configuration: \n {self.config}"

        self.notinkb_history = load_json(
            os.path.join(self.cache_dir, "notinkb_identifiers_history.json")
        )
        self.qaqc_stats = load_json(os.path.join(self.cache_dir, "qaqc_stats.json"))

        self.data :dict= {}
        assert self.config.splits is not None
        for split in self.config.splits:
            path = os.path.join(self.processed_directory, f"{split}.bioc.json")
            with open(path) as fp:
                collection = biocjson.load(fp)
            self.data[split] = [Example.from_bioc(d) for d in collection.documents]

    @property
    def cache_dir(self) -> str:
        """
        Store here stuff fetched from KB
        """

        cache_dir = [self.base_directory]

        assert self.config.entity_types is not None
        if len(self.config.entity_types) > 1:
            cache_dir.append(self.config.entity_type)

        if self.config.subsets is not None:
            assert self.config.subset is not None
            cache_dir.append(self.config.subset)

        path = os.path.join(*cache_dir)

        return path

    def __getitem__(self, key: str):
        return self.data.get(key)

    def __setitem__(self, key: Splits, value : list[Example]):
        assert isinstance(key, Splits), "Key can only be a `Split`"
        assert isinstance(next(iter(value)), Example), "Value can only be a `list[Example]`"
        self.data[key] = value

    def keys(self) -> list[str]:
        """
        Dict-like method
        """

        return list(self.data.keys())

    def values(self) -> list[list[Example]]:
        """
        Dict-like method
        """

        return list(self.data.values())

    def items(self) -> list[tuple]:
        """
        Dict-like method
        """

        return list(self.data.items())
