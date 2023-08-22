#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Interface to retrive annotated PubMed/PMC articles via the PubTator API"""

import os
import time
from collections import defaultdict
from typing import Iterator, Optional, Union

import bioc
import requests  # type: ignore
from bioc import biocjson, biocxml, pubtator
from loguru import logger
from omegaconf import OmegaConf
from sqlalchemy import types as sql_types
from sqlalchemy.schema import Column, Index, MetaData, Table
# from sqlalchemy.sql.expression import and_, select
from sqlalchemy.sql.expression import select

from belb.kbs.db import DatabaseConnection, SqlDialects
from belb.preprocessing.data import Entities
from belb.utils import StrEnum


class PubTatorDbTables(StrEnum):
    """
    Table in PubTator DB  built from `bioconcepts2pubtator.offsets`
    """

    PMCID_TO_PMID = "pmcid_to_pmid"
    ABSTRACTS = "abstracts"
    ANNOTATIONS = "annotations"


class PubTatorApiEntities(StrEnum):
    """
    Avalibale entities in PubTator API
    """

    GENE = "gene"
    DISEASE = "disease"
    CHEMICAL = "chemical"
    SPECIES = "species"
    MUTATION = "mutation"
    CELLLINE = "cellline"


def chunkize(items: list, chunksize: int) -> Iterator[list]:
    """
    Split list into chunks
    """

    for i in range(0, len(items), chunksize):
        yield items[i : i + chunksize]


class PubTatorSchema:
    """
    Schema of PubTator database built from `bioconcepts2pubtator.offsets`
    """

    def __init__(self, db_config: str):
        self.db_config = OmegaConf.load(db_config)
        # NOTE: sqlite has no schema
        if self.db_config.db.dialect == SqlDialects.SQLITE:
            self.db_config.schema.name = None
        self.schema_config = self.db_config.schema
        self.metadata = MetaData()

    def get_table_full_name(self, name: str) -> str:
        """
        Get table name
        """

        full_name = name

        schema_name = self.schema_config.get("name")
        if schema_name is not None:
            full_name = f"{schema_name}.{full_name}"

        return full_name

    def get(self, name: str, **kwargs) -> Table:
        """
        Call function according to name
        """

        full_name = self.get_table_full_name(name)

        if full_name not in self.metadata.tables:
            do = f"get_{name}"
            if hasattr(self, do) and callable(func := getattr(self, do)):
                outputs = func(**kwargs)
            else:
                raise ValueError(f"{self.__class__.__name__} has not method `{do}`!")
        else:
            outputs = self.metadata.tables[full_name]

        return outputs

    def get_pmcid_to_pmid(self):
        """
        Table `pmcid_to_pmid` to map PMC IDs to PubMed IDs
        """

        name = "pmcid_to_pmid"

        specs = [
            Column("pmcid", sql_types.Integer, primary_key=True),
            Column("pmid", sql_types.Integer, nullable=False),
        ]

        table = Table(
            name,
            self.metadata,
            schema=self.schema_config.get("name"),
            postgresql_tablespace=self.schema_config.get("postgresql_tablespace"),
            *specs,
        )

        return table

    def get_abstracts(self):
        """
        Table `abstracts` with PMIDs text
        """

        name = "abstracts"

        specs = [
            Column("pmid", sql_types.Integer, primary_key=True),
            Column("title", sql_types.Text, nullable=False),
            Column("abstract", sql_types.Text, nullable=True),
        ]

        table = Table(
            name,
            self.metadata,
            schema=self.schema_config.get("name"),
            postgresql_tablespace=self.schema_config.get("postgresql_tablespace"),
            *specs,
        )

        return table

    def get_annotations(self):
        """
        Table `annotations` with PMIDs annotations
        """

        name = "annotations"

        specs = [
            Column("pmid", sql_types.Integer, nullable=False),
            Column("type", sql_types.Text, nullable=False),
            Column("text", sql_types.Text, nullable=False),
            Column("start", sql_types.Integer, nullable=False),
            Column("end", sql_types.Integer, nullable=False),
            Index(
                "index_pmid",
                "pmid",
                postgresql_tablespace=self.schema_config.get("postgresql_tablespace"),
            ),
        ]

        table = Table(
            name,
            self.metadata,
            schema=self.schema_config.get("name"),
            postgresql_tablespace=self.schema_config.get("postgresql_tablespace"),
            *specs,
        )

        return table


class PubTatorDB(DatabaseConnection):
    """
    Interface to local PubTator DB built from `bioconcepts2pubtator.offsets`
    """

    def __init__(
        self,
        directory: str,
        db_config: str,
        entity_types: Optional[list] = None,
        debug: bool = False,
    ):
        self.directory = directory
        self.entity_types = entity_types if entity_types is not None else list(Entities)
        self.schema = PubTatorSchema(db_config=db_config)

        super().__init__(
            config=self.schema.db_config,
            debug=debug,
            file=os.path.join(directory, "pubtator.db"),
        )

    def init_database(self):
        """
        Initialize database
        """

        self.create_schema(self.schema.schema_config.get("name"))

        # init `MetaData` object
        for table in PubTatorDbTables:
            self.schema.get(table)

        self.schema.metadata.create_all(self.engine)

    def fetch_pmcid_to_pmid(self, pmcids: set[str]) -> dict:
        """
        Get mapping from PMCID to PMID
        """

        table = self.schema.get(PubTatorDbTables.PMCID_TO_PMID)

        query = select(table.c.pmcid, table.c.pmid).where(
            table.c.pmcid.in_(tuple(pmcids))
        )

        pmcid_to_pmid: dict = {}

        for row in self.query(query):
            pmcid_to_pmid[row["pmcid"]] = row["pmid"]

        return pmcid_to_pmid

    def fetch_annotations(
        self,
        ids: set[str],
        entity_types: Optional[list] = None,
    ) -> dict:
        """
        Fetch examples from local PubTator sqilte DB built from `bioconcepts2pubtator.offset`
        """

        table = self.schema.get(PubTatorDbTables.ANNOTATIONS)

        entity_types = entity_types if entity_types is not None else self.entity_types

        subquery = (
            select(table.c.pmid, table.c.type, table.c.text, table.c.start, table.c.end)
            .where(table.c.pmid.in_(tuple(ids)))
            .subquery("pmids")
        )

        query = select(subquery).where(subquery.c.type.in_(entity_types))

        annotations = defaultdict(list)
        for row in self.query(query):
            annotations[row.pop("pmid")].append(row)

        return dict(annotations)


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
        save_download_history: bool = False,
        download_history_file: Optional[str] = None,
    ):
        self.download_history_file = (
            download_history_file
            if download_history_file is not None
            else os.path.join(os.getcwd(), "history.txt")
        )
        self.save_download_history = save_download_history
        self.download_history = self.get_download_history()
        self.batch_size = batch_size
        self.full_text = full_text
        self.doc_format = doc_format
        self.download_delay = download_delay
        self.concepts = concepts if concepts is not None else list(PubTatorApiEntities)
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
        self, document_ids: set[Union[str, int]]
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

        if self.save_download_history:
            missing_ids = set(
                str(i) for i in document_ids if int(i) not in self.download_history
            )

        if len(missing_ids) > 0:
            logger.info("***Start FETCH process***")
            logger.info("Documents total: {}", len(document_ids))
            if self.save_download_history:
                logger.info("Download history file: `{}`", self.download_history_file)
                logger.info(
                    "Documents already downloaded: {}", len(self.download_history)
                )
            logger.info("Documents to be downloaded: {}", len(missing_ids))
            logger.info("Posting requests of batch size: {}", self.batch_size)

            total_request = 0

            if self.save_download_history:
                history_file = open(self.download_history_file, "a")

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
                    if self.save_download_history:
                        history_file.write(f"{document.id}\n")

                    yield document

            history_file.close()
