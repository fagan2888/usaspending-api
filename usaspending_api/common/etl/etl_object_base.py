from abc import abstractmethod, ABCMeta
from django.utils.functional import cached_property
from psycopg2.sql import Composed
from typing import List


class ETLObjectBase(metaclass=ABCMeta):
    """
    Represents a database object that has columns.  This will likely be table or view or
    common table expression (SQL query that returns results).  The goal is to abstract
    away much of the database introspection bits and encapsulate properties to reduce
    function call interfaces.
    """

    @cached_property
    def columns(self) -> List[str]:
        columns = self._get_columns()
        if not columns:
            raise RuntimeError("No columns found.  Do we have permission to see the database object?")
        return columns

    @cached_property
    def object_representation(self) -> Composed:
        return self._get_object_representation()

    @abstractmethod
    def _get_columns(self) -> List[str]:
        """ Returns the list of columns names represented by this object. """
        raise NotImplementedError("Must be implemented in subclasses of ETLObjectBase.")

    @abstractmethod
    def _get_object_representation(self) -> Composed:
        """ How this object should show up in queries.  Could be a name or subquery or whatever. """
        raise NotImplementedError("Must be implemented in subclasses of ETLObjectBase.")


__all__ = ["ETLObjectBase"]
