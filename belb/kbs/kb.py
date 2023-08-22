#!/usr/bin/env pythona
# -*- coding: utf-8 -*-
"""
Base interface to KB processing
"""
import argparse
import multiprocessing as mp
import os
from typing import Iterator, Optional

import pandas as pd
from loguru import logger
from smart_open import smart_open
from sqlalchemy.sql.expression import Selectable, bindparam, select, update

from belb.kbs.db import DatabaseConnection
from belb.kbs.parser import BaseKbParser
from belb.kbs.query import BelbKbQuery, Queries
from belb.kbs.schema import BelbKbSchema, Tables
from belb.preprocessing.data import (NA, CitationEntry, ForeignIdentifierEntry,
                                     IdentifierMappingEntry)
from belb.utils import CompressedFileWriter, chunkize, load_json, save_json


class KbConverter(CompressedFileWriter):
    """
    Convert KB into BELB format given a parser
    """

    def __init__(self, directory: str, parser: BaseKbParser, schema: BelbKbSchema):
        self.parser = parser
        self.schema = schema
        self.config = self.schema.kb_config
        self.root_directory = directory
        self.download_directory = os.path.join(
            self.root_directory, "raw", "kbs", self.config.name
        )
        os.makedirs(self.download_directory, exist_ok=True)
        self.processed_directory = os.path.join(
            self.root_directory, "processed", "kbs", self.config.name
        )
        os.makedirs(self.processed_directory, exist_ok=True)
        self.config.save(self.processed_directory)
        self.tables_directory = os.path.join(self.processed_directory, "tables")
        os.makedirs(self.tables_directory, exist_ok=True)

    @staticmethod
    def get_argument_parser(description: str) -> argparse.ArgumentParser:
        """
        Get argument parser for KB
        """

        parser = argparse.ArgumentParser(description=description)
        parser.add_argument(
            "--dir",
            required=True,
            type=str,
            help="Directory where all BELB data is stored",
        )
        parser.add_argument(
            "--db",
            required=True,
            type=str,
            help="Database configuration",
        )
        parser.add_argument(
            "--data_dir",
            default=None,
            type=str,
            help="Directory where raw data is stored",
        )
        parser.add_argument(
            "--overwrite",
            action="store_true",
            help="Overwrite data",
        )
        parser.add_argument(
            "--skip_kb", action="store_true", help="Skip creation of `kb` table"
        )
        parser.add_argument(
            "--skip_history",
            action="store_true",
            help="Skip creation of `history` table",
        )
        parser.add_argument(
            "--cores",
            type=int,
            default=min(mp.cpu_count(), 30),
            help="Available cores",
        )
        parser.add_argument(
            "--debug",
            action="store_true",
            help="Log level to DEBUG",
        )

        return parser

    def get_entries_generator(
        self, directory: str, name: str, cores: Optional[int] = None
    ) -> Optional[Iterator]:
        """
        Determine which entries to stream
        """

        entries_generator: Optional[Iterator] = None

        if name == Tables.KB:
            entries_generator = self.parser.parse_entries(
                directory=directory, cores=cores
            )
        elif name == Tables.HISTORY:
            entries_generator = self.parser.parse_history_entries(
                directory=directory, cores=cores
            )
        elif name == Tables.IDENTIFIER_MAPPING:
            entries_generator = (
                IdentifierMappingEntry(original_identifier=k, internal_identifier=v)
                for k, v in self.parser.identifier_mapping.items()
            )
        elif name == Tables.CITATIONS:
            entries_generator = (
                CitationEntry(pmid=pmid, identifier=i)
                for pmid, identifers in self.parser.citations.items()
                for i in identifers
            )
        elif name == Tables.FOREIGN_IDENTIFIERS:
            assert (
                len(self.parser.foreign_identifiers) > 0
            ), "KB with `foreign_identifier=True` but `parser.foreign_identifiers` was not populated!"
            entries_generator = (
                ForeignIdentifierEntry(i) for i in self.parser.foreign_identifiers
            )

        return entries_generator

    def write_table(
        self, name: str, cores: Optional[int] = None, log_every_n_entries: int = 10000
    ):
        """
        Write table data
        """

        logger.info("Start writing `{}` table file...", name)

        directory = (
            self.download_directory if not self.config.local else self.config.data_dir
        )

        assert (
            directory is not None
        ), "If KB is local only, you need to specify `data_dir`"

        entries_generator = self.get_entries_generator(
            name=name,
            directory=directory,
            cores=cores,
        )

        assert entries_generator is not None, f"Invalid table name `{name}`"

        with smart_open(
            os.path.join(self.tables_directory, f"{name}.tsv.gz"), "wb"
        ) as outfile:
            tot = 0

            for entry in entries_generator:
                if tot == 0:
                    outfile.write(self.get_line_from_tuple(entry.keys))

                outfile.write(self.get_line_from_tuple(entry.values))

                tot += 1

                if tot % log_every_n_entries == 0:
                    logger.info("#PROGRESS: written {} entries...", tot)

            if tot == 0:
                raise RuntimeError(
                    f"Table `{name}` is in schema, but parser did not provide any entries for the table!"
                )

        logger.info("Complted writing `{}` table: {} total entries.", name, tot)

        if name == Tables.KB:
            if len(self.parser.description_codes) == 0:
                raise RuntimeError(
                    "You must define a `mapping` from an entry description to its code (save disk space)"
                )
            save_json(
                item=self.parser.description_codes,
                path=os.path.join(self.processed_directory, "description_codes.json"),
                kwargs={"json": {"indent": 1}},
            )

    def to_belb(
        self,
        overwrite: bool = False,
        cores: Optional[int] = None,
        log_every_n_entries: int = 10000,
        skip_kb: bool = False,
        skip_history: bool = False,
    ):
        """
        Preprocess KB files.
        Create two files containing data for two tables:
            - kb : identifier, name, helper, is helper name
            - history : old_identifier, new_identifier
        """

        logger.info("Start converting {} to BELB format...", self.config.name)

        if not self.config.local:
            self.config.resource.download(directory=self.download_directory)

        desc_codes_path = os.path.join(
            self.processed_directory, "description_codes.json"
        )

        for table in self.schema.base_tables:
            if table == Tables.KB and skip_kb:
                continue
            if table == Tables.HISTORY and skip_history:
                continue

            path = os.path.join(self.tables_directory, f"{table}.tsv.gz")

            if os.path.exists(path) and not overwrite:
                logger.debug(
                    "File {} already exists. Skip table (pass `overwrite=True` for overwriting)...",
                    path,
                )
                continue

            self.write_table(
                name=table, cores=cores, log_every_n_entries=log_every_n_entries
            )

        if (
            not os.path.exists(desc_codes_path)
            and len(self.parser.description_codes) > 0
        ):
            raise ValueError("Parser did not define `description codes`!")

        save_json(
            path=desc_codes_path,
            item=self.parser.description_codes,
        )


def drop_duplicates(df: pd.DataFrame, subset: list) -> pd.DataFrame:
    """
    Remove duplicates from kb.
    Make sure preferred names are not tossed.
    """

    original_len = len(df)
    # sort to make sure that `symbol` (description=0) is on top
    df.sort_values(by=["identifier", "description", "name"], inplace=True)
    df.drop_duplicates(subset=subset, keep="first", inplace=True)
    logger.debug("Removed {} duplicate entries from KB.", original_len - len(df))

    return df


class BelbKb(DatabaseConnection):
    """
    Knowledge Base in BELB format
    """

    def __init__(
        self,
        directory: str,
        schema: BelbKbSchema,
        debug: bool = False,
    ):
        self.schema = schema
        self.kb_config = schema.kb_config

        self.root_directory = directory

        self.processed_directory = os.path.join(
            self.root_directory, "processed", "kbs", self.kb_config.name
        )

        self.queries = BelbKbQuery(schema=self.schema)

        self.tables_directory = os.path.join(self.processed_directory, "tables")

        description_codes_path = os.path.join(
            self.processed_directory, "description_codes.json"
        )
        assert os.path.exists(
            description_codes_path
        ), f"File {description_codes_path} not found. You need to convert the KB to BELB first `KbConverter.to_belb`"
        self.description_codes = load_json(description_codes_path)

        self.sentinel_file = os.path.join(self.processed_directory, "status.json")
        if not os.path.exists(self.sentinel_file):
            logger.info(
                "Database was not initialized. Call `init_database` before anything else..."
            )

        super().__init__(
            config=self.schema.db_config,
            debug=debug,
            file=os.path.join(self.processed_directory, "kb.db"),
        )

    def to_database(
        self,
        tables: list[Tables],
        chunksize: Optional[int] = None,
        dedup: bool = False,
    ):
        """
        Copy tables to database
        """

        if dedup and chunksize is not None:
            logger.info(
                "Deduplication w/ incremental upload (chunksize>0) not supported..."
            )

        dedup_subset = ["identifier", "name"]
        if self.kb_config.foreign_identifier:
            dedup_subset.append("foreign_identifier")

        for table_name in tables:
            table = self.schema.get(table_name)

            path = os.path.join(self.tables_directory, f"{table_name}.tsv.gz")

            read_csv_kwargs = {
                "filepath_or_buffer": path,
                # "na_filter": False,
                "keep_default_na": False,
                "na_values": [NA],
                "sep": "\t",
            }

            if chunksize is not None:
                if dedup:
                    logger.warning(
                        "Cannot perform deduplication when reading data by chunks (i.e. `chunksize>0`)"
                    )
                read_csv_kwargs["chunksize"] = chunksize
                reader = pd.read_csv(**read_csv_kwargs)
                for chunk in reader:
                    # chunk.dropna(subset=["name"], inplace=True)

                    self.populate_table(table=table, df=chunk)
            else:
                df = pd.read_csv(**read_csv_kwargs)
                if dedup and table_name == Tables.KB:
                    df = drop_duplicates(df=df, subset=dedup_subset)

                self.populate_table(table=table, df=df)

            logger.info(f"Completed populating table `{table_name}`")

        # # for AsyncEngine created in function scope, close and
        # # clean-up pooled connections
        # await self.db.engine.dispose()

    def init_database(self, chunksize: Optional[int] = None, dedup: bool = False):
        """
        Initiaize KB database
        """

        logger.info("Initilializing knowledge base database...")

        if os.path.exists(self.sentinel_file):
            raise RuntimeError(
                "Database exists! Delete the `status.json` and `kb.db` file (sqlite) or `drop` the tables in the schema (others)"
            )

        self.create_schema(self.schema.schema_config.get("name"))

        self.schema.init_metadata()

        self.schema.metadata.create_all(self.engine)

        self.to_database(
            tables=self.schema.base_tables, chunksize=chunksize, dedup=dedup
        )

        # self.to_database(
        #     tables=[Tables.KB], chunksize=chunksize, dedup=dedup
        # )

        save_json(path=self.sentinel_file, item={"status": "up"})

    def populate_table_from_query(
        self,
        query_name: str,
        table_name: str,
        query: Optional[Selectable] = None,
        chunksize: int = 10000,
    ):
        """
        Populate table from query
        """

        table = self.schema.get(table_name)

        if query is None:
            query = self.queries.get(query_name)

        data: list[dict] = []

        for row in self.query(query=query):
            parsed_row = self.queries.parse_result(name=query_name, row=row)

            rows = parsed_row if isinstance(parsed_row, list) else [parsed_row]

            for r in rows:
                if len(data) < chunksize:
                    data.append(r)
                else:
                    self.populate_table(table=table, df=pd.DataFrame(data))
                    data.clear()
                    data.append(r)

        if len(data) > 0:
            self.populate_table(table=table, df=pd.DataFrame(data))

    def get_notinkb(
        self, identifiers: set[str], chunksize: Optional[int] = None
    ) -> set[str]:
        """
        Get history of corpus identifiers which are not in KB
        """

        chunksize = chunksize if chunksize is not None else len(identifiers)

        inkb: set = set()

        table = self.schema.get(Tables.IDENTIFIER_HOMONYMS)
        for row in self.query(select(table.c.homonym)):
            inkb.add(row["homonym"])

        for batch in chunkize(identifiers, chunksize=chunksize):
            query = self.queries.get(Queries.INKB, identifiers=list(batch))

            for row in self.query(query):
                parsed_row = self.queries.parse_result(name=Queries.INKB, row=row)
                assert isinstance(parsed_row, dict)
                inkb.add(str(parsed_row["identifier"]))

        notinkb = set(i for i in identifiers if i not in inkb)

        return notinkb

    def get_notinkb_history(
        self, identifiers: set, chunksize: Optional[int] = None
    ) -> dict:
        """
        Retrive history of identifiers not-in-kb.
        """

        notinkb = self.get_notinkb(identifiers=identifiers, chunksize=chunksize)

        chunksize = chunksize if chunksize is not None else len(identifiers)

        notinkb_history: dict = {}

        if self.kb_config.history:
            table = self.schema.get(Tables.HISTORY)

            for batch in chunkize(notinkb, chunksize=chunksize):
                query = select(table.c.old_identifier, table.c.new_identifier).where(
                    table.c.old_identifier.in_(list(batch))
                )

                for row in self.query(query):
                    notinkb_history[row["old_identifier"]] = str(row["new_identifier"])

            for i in notinkb:
                if i not in notinkb_history:
                    notinkb_history[i] = NA

        else:
            notinkb_history.update({i: NA for i in notinkb})

        return notinkb_history

    def save_notinkb_history(
        self, path: str, identifiers: set[str], chunksize: Optional[int] = None
    ):
        """
        Save `history` for corpus identifier which are not-in-kb
        """

        notinkb_history = self.get_notinkb_history(
            identifiers=identifiers, chunksize=chunksize
        )

        save_json(path=path, item=notinkb_history, kwargs={"json": {"indent": 1}})

    def get_identifier_homonyms(self, identifiers: set) -> dict:
        """
        Mapping for identifiers in `identifier_homonyms`
        """

        table = self.schema.get(Tables.IDENTIFIER_HOMONYMS)
        query = select(table.c.homonym, table.c.identifier).where(
            table.c.homonym.in_(list(identifiers))
        )

        mapping: dict = {}
        for row in self.query(query):
            mapping[row["homonym"]] = row["identifier"]

        return mapping

    def get_reverse_identifier_homonyms(self, identifiers: set) -> dict:
        """
        Extend identifier to its homonyms list
        """

        table = self.schema.get(Tables.IDENTIFIER_HOMONYMS)
        query = select(table.c.identifer, table.c.homonym).where(
            table.c.identifer.in_(list(identifiers))
        )

        mapping: dict = {}
        for row in self.query(query):
            h = row["homonym"]
            i = row["identifier"]
            if i not in mapping:
                mapping[i] = []

            mapping[i].append(h)

        return mapping

    def get_identifier_mapping(self, identifiers: set) -> dict:
        """
        Convert corpus identifiers to integers
        """

        table = self.schema.get(Tables.IDENTIFIER_MAPPING)
        query = select(table.c.internal_identifier, table.c.original_identifier).where(
            table.c.original_identifier.in_(list(identifiers))
        )
        mapping: dict = {}
        for row in self.query(query):
            mapping[row["original_identifier"]] = row["internal_identifier"]
        return mapping

    def get_reverse_identifier_mapping(self, identifiers: set) -> dict:
        """
        Convert BELB identifier back to original ones
        """

        table = self.schema.get(Tables.IDENTIFIER_MAPPING)
        query = select(table.c.internal_identifier, table.c.original_identifier).where(
            table.c.internal_identifier.in_(list(identifiers))
        )

        mapping: dict = {}
        for row in self.query(query):
            iid = row["internal_identifier"]
            oid = row["original_identifier"]
            if iid not in mapping:
                mapping[iid] = []
            mapping[iid].append(oid)

        return mapping

    def get_history_foreign_identifiers(
        self, foreign_identifiers: set, foreign_kb: "BelbKb"
    ) -> dict:
        """
        Get history of foreign identiifers from `foreign_kb`
        """

        history_table = foreign_kb.schema.get(Tables.HISTORY)

        query = select(
            history_table.c.new_identifier,
            history_table.c.old_identifier,
        ).where(history_table.c.old_identifier.in_(foreign_identifiers))

        mapping: dict = {}
        for row in foreign_kb.query(query):
            if row["new_identifier"] == -1:
                if str(row["old_identifier"]) not in self.kb_config.foreign_patch:
                    logger.warning(
                        """
                        Foreign identifier `{}` has been discontinued and is not in `config.foreign_path`.
                        Please make sure to include add it in and re-build this kb
                        """,
                        row["old_identifier"],
                    )
                    continue
            mapping[str(row["old_identifier"])] = str(row["new_identifier"])

        return mapping

    def update_foreign_identifiers(self, foreign_kb: "BelbKb", chunksize: int = 10000):
        """
        add names to FOREIGN_IDENTIFIERS table w/ foreign_kb
        """

        logger.info("Updating `foreign_identifiers` table with names")

        table = self.schema.get(Tables.FOREIGN_IDENTIFIERS)
        stmt = (
            update(table)
            .where(table.c.identifier == bindparam("_identifier"))
            .values(name=bindparam("name"))
        )

        # patch of names missing from foreign kb
        # given directly from original kb: see Cellosaurus
        data = [
            {"_identifier": k, "name": v}
            for k, v in self.kb_config.foreign_patch.items()
        ]
        if len(data) > 0:
            self.connection.execute(stmt, data)
            data.clear()

        fids = set(str(r["identifier"]) for r in self.query(select(table.c.identifier)))

        with foreign_kb as handle:
            fids_history = self.get_history_foreign_identifiers(
                foreign_identifiers=fids, foreign_kb=foreign_kb
            )

            fids_inverted_history = {v: k for k, v in fids_history.items()}

            mapped_fids = set(fids_history.get(fid, fid) for fid in fids)

            query = foreign_kb.queries.get(
                name=Queries.IDENTIFIER_PREFERRED_NAME,
                identifiers=mapped_fids,
            )

            for rows in chunkize(handle.query(query), chunksize):
                parsed_rows = [
                    foreign_kb.queries.parse_result(
                        name=Queries.IDENTIFIER_PREFERRED_NAME, row=r
                    )
                    for r in rows
                ]

                for r in parsed_rows:
                    assert isinstance(r, dict)
                    # https://docs.sqlalchemy.org/en/14/tutorial/data_update.html#updating-and-deleting-rows-with-core
                    identifier = r.pop("identifier")
                    # if identifier has been replaced convert it back to the original one
                    r["_identifier"] = fids_inverted_history.get(
                        str(identifier), str(identifier)
                    )

                self.connection.execute(stmt, parsed_rows)

        self.connection.commit()

    def save_query_result(self, query: Selectable, path: str):
        """
        Save query results in TSV file
        """

        with open(path, "w") as fp:
            for idx, row in enumerate(self.query(query)):
                keys = sorted(row.keys())
                if idx == 0:
                    line = "\t".join(keys)
                    fp.write(f"{line}\n")
                line = "\t".join(str(row[k]) for k in keys)
                fp.write(f"{line}\n")
