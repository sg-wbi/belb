#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BELB KB queries
"""

from typing import Optional, Union

from sqlalchemy import func as sql_func
from sqlalchemy.sql.expression import Selectable, and_, distinct, select

from belb.kbs.db import SqlDialects
from belb.kbs.schema import BelbKbSchema, Tables
from belb.preprocessing.data import SYMBOL_CODE
from belb.utils import StrEnum


class Queries(StrEnum):
    """
    Available predefined queries
    """

    IDENTIFIER_HOMONYMS = "identifier_homonyms"
    FOREIGN_NAME_HOMONYMS = "foreign_name_homonyms"
    NAME_HOMONYMS = "name_homonyms"
    IDENTIFIER_PREFERRED_NAME = "identifier_preferred_name"
    INKB = "inkb"
    SYNSET = "synset"


class BelbKbQuery:
    """
    Create queries
    """

    def __init__(self, schema: BelbKbSchema):
        self.schema = schema
        self.dialect = self.schema.db_config.db.dialect
        self.connector = "||"

    def aggregate(self, *args):
        """
        Aggregate columns values
        """
        if self.dialect == SqlDialects.POSTGRESQL:
            return sql_func.array_agg(*args)
        if self.dialect == SqlDialects.SQLITE:
            return sql_func.group_concat(*args, self.connector)
        raise ValueError(
            f"Cannot determine aggregation function for dialect `{self.dialect}`"
        )

    def unpack_aggregated_values(self, values: Union[list, str]) -> list:
        """
        Safe unpacking: PostreSQL can handle arrays, while sqlite not!
        """

        # splitting on commas is dangerous!
        # 461058|390140|5|Vitis bryoniifolia Bunge, 1833|
        # becomes " 1833"
        if isinstance(values, str):
            unpacked = values.split(self.connector)
        else:
            unpacked = values

        return unpacked

    def get(self, name: str, **kwargs) -> Selectable:
        """
        Call function according to name
        """

        assert name in Queries, f"Predefined query `{name}` was not implemented"

        do = f"get_{name}"
        if hasattr(self, do) and callable(func := getattr(self, do)):
            outputs = func(**kwargs)
        else:
            raise ValueError(f"{self.__class__.__name__} has not method `{do}`!")

        return outputs

    def parse_result(self, name: str, row: dict) -> Union[dict, list[dict]]:
        """
        Call function according to name
        """

        do = f"parse_result_{name}"
        if hasattr(self, do) and callable(func := getattr(self, do)):
            outputs = func(row)
        else:
            # no special handling, just return result
            outputs = row

        return outputs

    def get_identifier_homonyms(
        self, foreign_identifiers: Optional[list] = None
    ) -> Selectable:
        """
        select attribute, foreign_identifier, group_concat(identifier), names from
            (select attribute, foreign_identifier, identifier, group_concat(name) as names from
                (select foreign_identifier, identifier, name from cellosaurus_kb
                 where foreign_identifier in (?,?) order by identifier, name)
             group by attribute, foreign_identifier, identifier)
        group by attribute, foreign_identifier, names having count(distinct(identifier))>1;
        """

        table = self.schema.get(Tables.KB)

        columns = [table.c.name, table.c.identifier]
        if self.schema.kb_config.foreign_identifier:
            columns.append(table.c.foreign_identifier)
        if self.schema.kb_config.attribute:
            columns.append(table.c.attribute)

        # (select foreign_identifier, identifier, name from cellosaurus_kb where foreign_identifier in (?,?,?) order by identifier, name)
        if self.schema.kb_config.foreign_identifier and foreign_identifiers is not None:
            sorted_table = (
                select(*columns)
                .where(table.c.foreign_identifier.in_(foreign_identifiers))
                .order_by(table.c.identifier, table.c.description, table.c.name)
                .subquery("sorted")
            )
        # (select foreign_identifier, identifier, name from cellosaurus_kb order by identifier, name)
        else:
            sorted_table = (
                select(*columns)
                .order_by(table.c.identifier, table.c.description, table.c.name)
                .subquery("sorted")
            )

        # (select attribute, foreign_identifier, identifier, group_concat(name) as names from
        #     (select attribute, foreign_identifier, identifier, name from cellosaurus_kb order by identifier, name)
        #  group by attribute, foreign_identifier, identifier)
        columns = [
            sorted_table.c.identifier,
            self.aggregate(sorted_table.c.name).label("names"),
        ]
        group_by = [sorted_table.c.identifier]
        if self.schema.kb_config.foreign_identifier:
            columns.append(sorted_table.c.foreign_identifier)
            group_by.append(sorted_table.c.foreign_identifier)
        if self.schema.kb_config.attribute:
            columns.append(sorted_table.c.attribute)
            group_by.append(sorted_table.c.attribute)
        grouped_names_table = (
            select(*columns).group_by(*group_by).subquery("grouped_names")
        )

        # select foreign_identifier,group_concat(identifier), names from
        #     (select foreign_identifier, identifier, group_concat(name) as names from
        #         (select foreign_identifier, identifier, name from cellosaurus_kb order by identifier, name)
        #      group by foreign_identifier, identifier)
        # group by attribute, foreign_identifier, names having count(distinct(identifier))>1;
        columns = [
            self.aggregate(grouped_names_table.c.identifier).label("identifiers"),
            grouped_names_table.c.names,
        ]
        group_by = [grouped_names_table.c.names]
        if self.schema.kb_config.foreign_identifier:
            columns.append(grouped_names_table.c.foreign_identifier)
            group_by.append(grouped_names_table.c.foreign_identifier)
        if self.schema.kb_config.attribute:
            columns.append(
                self.aggregate(grouped_names_table.c.attribute).label("attributes")
            )
            group_by.append(grouped_names_table.c.attribute)
        query = (
            select(*columns)
            .group_by(*group_by)
            .having(sql_func.count(distinct(grouped_names_table.c.identifier)) > 1)
        )

        return query

    def parse_result_identifier_homonyms(self, row: dict) -> Union[dict, list[dict]]:
        """
        Unpack result from Queries.IDENTIFIER_HOMONYMS
        """

        identifiers = self.unpack_aggregated_values(row["identifiers"])
        if row.get("attributes") is not None:
            attributes = self.unpack_aggregated_values(row["attributes"])
            assert (
                len(set(attributes)) == 1
            ), f"Query `IDENTIFIER_HOMONYMS` failed! `attribute` should be the same for all, but found: {row}"

        # take the first one: all homonyms will be mapped to this
        value = identifiers.pop(0)

        rows = [{"homonym": k, "identifier": value} for k in identifiers]

        return rows

    def get_foreign_name_homonyms(self) -> Selectable:
        """
        select name, group_concat(uid) from cellosaurus_kb
        group by name having count(*)>1 and count(distinct(foreign_identifier))>1;
        """

        kb = self.schema.get(Tables.KB)

        query = (
            select(
                self.aggregate(kb.c.foreign_identifier).label("identifiers"),
                self.aggregate(kb.c.uid).label("uids"),
            )
            .group_by(kb.c.name)
            .having(
                and_(
                    sql_func.count("*") > 1,
                    sql_func.count(distinct(kb.c.foreign_identifier)) > 1,
                )
            )
        )

        return query

    def parse_result_foreign_name_homonyms(self, row: dict) -> list[dict]:
        """
        Unpack result from Queries.NAME_HOMONYMS_FOREIGN_IDENTIFIER
        """

        uids = self.unpack_aggregated_values(row["uids"])
        identifiers = self.unpack_aggregated_values(row["identifiers"])

        rows = []
        for uid, fi in zip(uids, identifiers):
            rows.append({"uid": uid, "identifier": fi})

        return rows

    def get_name_homonyms(
        self, foreign_identifiers: Optional[list] = None
    ) -> Selectable:
        """
        Query to get name homonyms (same name, different identifier)
        """

        table = self.schema.get(Tables.KB)

        columns = [
            table.c.name,
            self.aggregate(table.c.identifier).label("identifiers"),
            self.aggregate(table.c.uid).label("uids"),
            self.aggregate(table.c.description).label("descriptions"),
        ]

        group_by = [table.c.name]
        if self.schema.kb_config.foreign_identifier:
            columns.append(
                self.aggregate(table.c.foreign_identifier).label("foreign_identifiers")
            )
            group_by.append(table.c.foreign_identifier)

        if self.schema.kb_config.foreign_identifier and foreign_identifiers is not None:
            query = (
                select(*columns)
                .where(table.c.foreign_identifier.in_(foreign_identifiers))
                .group_by(*group_by)
                .having(sql_func.count("*") > 1)
            )
        else:
            query = select(*columns).group_by(*group_by).having(sql_func.count("*") > 1)

        return query

    def parse_result_name_homonyms(self, row: dict) -> list[dict]:
        """
        Unpack result from Queries.NAME_HOMONYMS
        """

        identifiers = self.unpack_aggregated_values(row["identifiers"])
        uids = self.unpack_aggregated_values(row["uids"])
        descriptions = self.unpack_aggregated_values(row["descriptions"])
        foreign_identifiers = (
            self.unpack_aggregated_values(row["foreign_identifiers"])
            if "foreign_identifiers" in row
            else None
        )

        rows = []

        for idx, (i, u, d) in enumerate(zip(identifiers, uids, descriptions)):
            row = {"name": row["name"], "identifier": i, "uid": u, "description": d}
            if foreign_identifiers is not None:
                row.update({"foreign_identifier": foreign_identifiers[idx]})
            rows.append(row)

        return rows

    def get_inkb(self, identifiers: list) -> Selectable:
        """
        Find identifiers which are in kb
        """

        if self.schema.kb_config.string_identifier:
            table = self.schema.get(Tables.IDENTIFIER_MAPPING)
            query = select(distinct(table.c.original_identifier)).where(
                table.c.original_identifier.in_(identifiers)
            )
        else:
            table = self.schema.get(Tables.KB)
            query = select(distinct(table.c.identifier)).where(
                table.c.identifier.in_(identifiers)
            )

        return query

    def parse_result_inkb(self, row: dict) -> dict:
        """
        Get identifier
        """

        if "original_identifier" in row:
            row["identifier"] = row.pop("original_identifier")

        return row

    def get_synset(self, subset: Optional[list] = None) -> Selectable:
        """
        Fetch synset (symbol + synonyms) along with descriptions
        """

        table = self.schema.get(Tables.KB)

        columns = [
            table.c.identifier,
            self.aggregate(table.c.name).label("names"),
            self.aggregate(table.c.description).label("descriptions"),
        ]

        if self.schema.kb_config.foreign_identifier:
            columns.append(table.c.foreign_identifier)

        if self.schema.kb_config.foreign_identifier and subset is not None:
            query = (
                select(*columns)
                .where(table.c.foreign_identifier.in_(subset))
                .group_by(table.c.identifier)
            )
        else:
            query = select(*columns).group_by(table.c.identifier)

        return query

    def parse_result_synset(self, row: dict) -> dict:
        """
        Parse result from query `get_synset`
        """

        row["names"] = self.unpack_aggregated_values(row["names"])
        row["descriptions"] = self.unpack_aggregated_values(row["descriptions"])

        return row
