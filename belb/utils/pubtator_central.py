#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Interface to retrive annotated PubMed/PMC articles via the PubTator API"""

import os
import sqlite3
import time
from collections import defaultdict
from typing import Iterator, Optional, Union

import bioc
import pandas as pd
import requests  # type: ignore
from bioc import biocjson, biocxml, pubtator
from loguru import logger
from pandas.io.parsers import TextFileReader

PUBTATOR_API_ENTITIES = [
    "gene",
    "disease",
    "chemical",
    "species",
    "mutation",
    "cellline",
]

PUBTATOR_DB_ENTITIES = ["gene", "species", "disease", "chemical", "variant"]


def chunkize(items: list, chunksize: int) -> Iterator[list]:
    """
    Split list into chunks
    """

    for i in range(0, len(items), chunksize):
        yield items[i : i + chunksize]


class PubTatorDB:
    """
    Interface to local PubTator DB built from `bioconcepts2pubtator.offsets`
    """

    def __init__(
        self,
        path: str,
        entity_types: Optional[list] = None,
    ):

        self.path = path
        self.entity_types = (
            entity_types if entity_types is not None else PUBTATOR_DB_ENTITIES
        )
        self.connection: Optional[sqlite3.Connection] = None

    def __enter__(self):

        self.connection = sqlite3.connect(self.path)

        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.connection.close()

    @staticmethod
    def group_annotations_by_pmid(result: pd.DataFrame) -> dict:
        """
        Group annotation table by PMID
        """

        d = defaultdict(list)

        for _, row in result.iterrows():
            row = row.to_dict()
            d[str(row.pop("pmid"))].append(row)

        return dict(d)

    @staticmethod
    def group_text_by_pmid(result: pd.DataFrame) -> dict:
        """
        Group text table by PMID
        """

        d: dict = {}

        for _, row in result.iterrows():
            row = row.to_dict()
            d[str(row.pop("pmid"))] = row

        return d

    def fetch_pmcid_to_pmid(self, pmcids: set[str]) -> dict:
        """
        Get mapping from PMCID to PMID
        """

        df = pd.read_sql(
            f"select pmcid,pmid from pmcid_pmid where pmcid in {tuple(pmcids)}",
            self.connection,
        )

        df = df.astype(str)

        pmcid_to_pmid = dict(zip(list(df["pmcid"]), list(df["pmid"])))

        return pmcid_to_pmid

    def fetch_texts(
        self, ids: set[str], chunksize: Optional[int] = None
    ) -> Union[pd.DataFrame, TextFileReader]:
        """
        Fetch text from the local PubTator DB
        """

        reader_or_df = pd.read_sql(
            f"select * from pubt_articles where pmid in {tuple(ids)} order by pmid",
            self.connection,
            chunksize=chunksize,
        )

        return reader_or_df

    def _get_query_annotations(self, ids: set[str], entity_types: list) -> str:

        queries = []

        for idx, entity_type in enumerate(entity_types):

            if idx != 0:

                queries.append(" UNION ")

            queries.append(
                f" SELECT pmid,type,text,start,end FROM pubt_{entity_type.capitalize()} WHERE pmid IN {tuple(ids)} "
            )

        query = " ".join(queries) + "order by pmid"

        return query

    def fetch_annotations(
        self,
        ids: set[str],
        entity_types: Optional[list] = None,
        chunksize: Optional[int] = None,
    ) -> Iterator[pd.DataFrame]:
        """
        Fetch examples from local PubTator sqilte DB built from `bioconcepts2pubtator.offset`
        """

        entity_types = entity_types if entity_types is not None else self.entity_types

        query = self._get_query_annotations(ids=ids, entity_types=entity_types)

        reader_or_df = pd.read_sql(query, self.connection, chunksize=chunksize)

        return reader_or_df


class PubTatorAPI:
    """
    Helper class to fetch annotated documents via the PubTator API:
        https://www.ncbi.nlm.nih.gov/research/pubtator/api.html
    """

    def __init__(
        self,
        full_text: bool = False,
        doc_format: str = "biocxml",
        batch_size: int = 1000,
        concepts: Optional[list[str]] = None,
        download_delay: float = 0.001,
        download_history_file: Optional[str] = None,
    ):

        if doc_format == "pubtator" and full_text:
            raise ValueError(
                "Full text articles are not supported in `pubtator` format!"
            )

        self.download_history_file = (
            download_history_file
            if download_history_file is not None
            else os.path.join(os.getcwd(), "cache.txt")
        )
        self.download_history = self.get_download_history()
        self.batch_size = batch_size
        self.full_text = full_text
        self.doc_format = doc_format
        self.download_delay = download_delay
        self.concepts = concepts if concepts is not None else PUBTATOR_API_ENTITIES
        self.url = (
            "https://www.ncbi.nlm.nih.gov/research/pubtator-api/publications/export/"
        )

    def get_download_history(self) -> set:
        """
        Load cache with already downloaded documents
        """

        cache = set()

        if os.path.exists(self.download_history_file):

            with open(self.download_history_file) as infile:
                for line in infile:
                    eid = int(line.strip())
                    cache.add(eid)

        return cache

    def prepare_document_ids(self, eids: list[Union[str, int]]) -> list[str]:
        """
        Apped PMC in case of full text
        """

        identifiers = [str(eid) for eid in eids]

        if self.full_text:
            identifiers = [f"PMC{eid}" for eid in eids]

        return identifiers

    def post_request(self, document_ids: list[Union[str, int]]) -> str:
        """
        POST request via PubTator API to fetch annotated documents.

        Parameters
        ----------
        eids : List[Union[str, int]]
            Document identifiers to be retrieved

        Returns
        -------
        str
            PubTator API response: string containing annotated documents
        """

        json_data = {}
        json_data["concepts"] = self.concepts
        document_ids_type = "pmcids" if self.full_text else "pmids"
        json_data[document_ids_type] = self.prepare_document_ids(document_ids)

        res = requests.post(self.url + self.doc_format, json=json_data)

        if res.status_code != 200:
            logger.error("POST failed wit payload {}", json_data)
            response = ""
        else:
            response = res.text

        return response

    def parse(
        self, response: str
    ) -> Union[list[pubtator.PubTator], list[bioc.BioCDocument]]:
        """
        Parse fetched text via PubTator API.

        Parameters
        ----------
        response : str
            Response from PubTator API

        Returns
        -------
        Union[List[str], List[bioc.BioCDocument]]
            List of annotated documents
        """

        if self.doc_format == "pubtator":

            documents = pubtator.loads(response)

        elif self.doc_format == "biocxml":

            collection = biocxml.loads(response)

            documents = collection.documents

        elif self.doc_format == "biocjson":

            collection = biocjson.loads(response)

            documents = collection.documents

        return documents

    def fetch(
        self, document_ids: set
    ) -> Iterator[Union[pubtator.PubTator, bioc.BioCDocument]]:
        """
        Fetch annotated documents by identifier.

        Parameters
        ----------
        eids : Set[Union[str, int]]
            Document identifiers

        Returns
        -------
        Iterator[Union[str, bioc.BioCDocument]]
            Generator of annotated documents
        """

        missing_ids = set(str(i) for i in document_ids)

        missing_ids = set(
            str(i) for i in document_ids if int(i) not in self.download_history
        )

        if len(missing_ids) > 0:

            logger.info("***Start FETCH process***")
            logger.info("Documents total: {}", len(document_ids))
            logger.info("Download history file: `{}`", self.download_history_file)
            logger.info("Documents already downloaded: {}", len(self.download_history))
            logger.info("Documents to be downloaded: {}", len(missing_ids))
            logger.info("Posting requests of batch size: {}", self.batch_size)

            total_request = 0

            with open(self.download_history_file, "a") as fp:

                for batch in chunkize(list(missing_ids), chunksize=self.batch_size):

                    batch = list(batch)

                    response = self.post_request(batch)

                    time.sleep(60 * self.download_delay)

                    if not len(response) > 0:
                        continue

                    documents = self.parse(response)

                    if (total_request + 1) % (self.batch_size * 10) == 0:
                        logger.info("Processed  {} documents...", total_request)

                    total_request += len(batch)

                    for document in documents:

                        document_id = (
                            document.pmid
                            if self.doc_format == "pubtator"
                            else document.id
                        )

                        fp.write(f"{document_id}\n")

                        yield document
