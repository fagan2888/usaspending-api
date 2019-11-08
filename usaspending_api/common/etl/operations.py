""" Functions to generate SQL for several higher level ETL operations. """


from psycopg2.sql import SQL
from typing import List
from usaspending_api.common.etl import ETLObjectBase, ETLWritableObjectBase, ETLTemporaryTable, primatives
from usaspending_api.common.helpers import sql_helpers


def _get_shared_columns(source_columns: List[str], destination_columns: List[str]) -> List[str]:
    """ Return the list of columns contained in both lists. """

    shared_columns = [c for c in destination_columns if c in source_columns]
    if not shared_columns:
        raise RuntimeError("No shared columns between database objects.")
    return shared_columns


def _get_changeable_columns(source: ETLObjectBase, destination: ETLWritableObjectBase):
    """ Destination columns that are in source that are not overridden and are not keys. """
    changeable_columns = [
        c
        for c in destination.columns
        if c in source.columns and c not in destination.update_overrides and c not in destination.key_columns
    ]
    if not changeable_columns:
        raise RuntimeError("No changeable columns.")
    return changeable_columns


def _get_settable_columns(source: ETLObjectBase, destination: ETLWritableObjectBase):
    """ Destination columns that are in source or are overridden but are not keys. """
    settable_columns = [
        c
        for c in destination.columns
        if (c in source.columns or c in destination.update_overrides) and c not in destination.key_columns
    ]
    if not settable_columns:
        raise RuntimeError("No settable columns.")
    return settable_columns


def delete_obsolete_rows(source: ETLObjectBase, destination: ETLWritableObjectBase) -> int:
    """ Delete rows from destination that do not exist in source and return the number of rows deleted. """

    sql = """
        delete from {destination_object_representation}
        where not exists (
            select from {source_object_representation} s where {join}
        )
    """

    sql = SQL(sql).format(
        destination_object_representation=destination.object_representation,
        source_object_representation=source.object_representation,
        join=primatives.make_join_to_table_conditional(destination.key_columns, "s", destination.object_representation),
    )

    return sql_helpers.execute_dml_sql(sql)


def identify_new_or_updated(
    source: ETLObjectBase, destination: ETLWritableObjectBase, staging: ETLTemporaryTable
) -> int:
    """
    Create a temporary staging table containing keys of rows in source that are new or
    updated from destination and return the number of rows affected.
    """

    # Destination columns that are in source that are not overridden and are not keys.
    changeable_columns = _get_changeable_columns(source, destination)

    sql = """
        create temporary table {staging_object_representation} as
        select {select_columns}
        from   {source_object_representation} as s
               left outer join {destination_object_representation} as d on {join}
        where  ({excluder}) or ({detect_changes})
    """

    sql = SQL(sql).format(
        staging_object_representation=staging.object_representation,
        select_columns=primatives.make_column_list([c.name for c in destination.key_columns], "s"),
        source_object_representation=source.object_representation,
        destination_object_representation=destination.object_representation,
        join=primatives.make_join_conditional(destination.key_columns, "d", "s"),
        excluder=primatives.make_join_excluder_conditional(destination.key_columns, "d"),
        detect_changes=primatives.make_change_detector_conditional(changeable_columns, "s", "d"),
    )

    return sql_helpers.execute_dml_sql(sql)


def insert_missing_rows(source: ETLObjectBase, destination: ETLWritableObjectBase) -> int:
    """ Insert rows from source that do not exist in destination and return the number of rows inserted. """

    # Destination columns that are in source or are overridden.
    insertable_columns = _get_shared_columns(source.columns + list(destination.insert_overrides), destination.columns)

    sql = """
        insert into {destination_object_representation} ({insert_columns})
        select      {select_columns}
        from        {source_object_representation} as s
                    left outer join {destination_object_representation} as d on {join}
        where       {excluder}
    """

    sql = SQL(sql).format(
        destination_object_representation=destination.object_representation,
        insert_columns=primatives.make_column_list(insertable_columns),
        select_columns=primatives.make_column_list(insertable_columns, "s", destination.insert_overrides),
        source_object_representation=source.object_representation,
        join=primatives.make_join_conditional(destination.key_columns, "d", "s"),
        excluder=primatives.make_join_excluder_conditional(destination.key_columns, "d"),
    )

    return sql_helpers.execute_dml_sql(sql)


def stage_table(source: ETLObjectBase, destination: ETLWritableObjectBase, staging: ETLTemporaryTable) -> int:
    """ Copy source table contents to staging table and return the number of rows copied. """

    shared_columns = _get_shared_columns(source.columns, destination.columns)

    sql = """
        create temporary table {staging_object_representation} as
        select {select_columns}
        from   {source_object_representation} as t
    """

    sql = SQL(sql).format(
        staging_object_representation=staging.object_representation,
        select_columns=primatives.make_column_list(shared_columns),
        source_object_representation=source.object_representation,
    )

    return sql_helpers.execute_dml_sql(sql)


def update_changed_rows(source: ETLObjectBase, destination: ETLWritableObjectBase) -> int:
    """ Update rows in destination that have changed in source and return the number of rows updated. """

    # Destination columns that are in source or are overridden but are not keys.
    settable_columns = _get_settable_columns(source, destination)

    # Destination columns that are in source that are not overridden and are not keys.
    changeable_columns = _get_changeable_columns(source, destination)

    sql = """
        update      {destination_object_representation} as d
        set         {set}
        from        {source_object_representation} as s
        where       {where} and ({detect_changes})
    """

    sql = SQL(sql).format(
        destination_object_representation=destination.object_representation,
        set=primatives.make_column_setter_list(settable_columns, "s", destination.update_overrides),
        source_object_representation=source.object_representation,
        where=primatives.make_join_conditional(destination.key_columns, "s", "d"),
        detect_changes=primatives.make_change_detector_conditional(changeable_columns, "s", "d"),
    )

    return sql_helpers.execute_dml_sql(sql)


__all__ = [
    "delete_obsolete_rows",
    "identify_new_or_updated",
    "insert_missing_rows",
    "stage_table",
    "update_changed_rows",
]
