from psycopg2.sql import Composed, Identifier, SQL
from typing import List
from usaspending_api.common.etl import primatives
from usaspending_api.common.etl.etl_object_base import ETLObjectBase
from usaspending_api.common.etl.introspection import get_columns
from usaspending_api.common.etl.primatives import DataTypes


class ETLDBLinkTable(ETLObjectBase):
    """ Represents a remote permanent database table accessed via dblink. """

    def __init__(self, table_name: str, dblink_name: str, data_types: DataTypes, schema_name: str = "public") -> None:
        self.table_name = table_name
        self.schema_name = schema_name
        self.dblink_name = dblink_name
        self.data_types = data_types
        super(ETLDBLinkTable, self).__init__()

    def _get_columns(self) -> List[str]:
        return get_columns(self.table_name, self.schema_name, self.dblink_name)

    def _get_object_representation(self) -> Composed:
        """ To help us treat a dblink table like any other object, let's wrap it in a subquery. """
        remote_table = SQL("{}.{}").format(Identifier(self.schema_name), Identifier(self.table_name))
        remote_sql = SQL("select {columns} from {remote_table}").format(
            columns=primatives.make_column_list(self.columns), remote_table=remote_table
        )
        return SQL("({})").format(
            primatives.wrap_dblink_query(self.dblink_name, remote_sql, "r", self.columns, self.data_types)
        )


__all__ = ["ETLDBLinkTable"]
