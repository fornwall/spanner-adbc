"""DBAPI 2.0 and Arrow result helpers for Spanner.

Connections buffer writes until ``commit()`` unless opened with
``autocommit=True``. Closing discards uncommitted writes.
"""

from collections.abc import Mapping

import adbc_driver_manager.dbapi

# Spanner has no TIME type, so `Time` values can't be bound.
from adbc_driver_manager.dbapi import (
    BINARY,
    DATETIME,
    NUMBER,
    ROWID,
    STRING,
    Connection,
    Cursor,
    DataError,
    DatabaseError,
    Date,
    DateFromTicks,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    Time,
    TimeFromTicks,
    Timestamp,
    TimestampFromTicks,
    Warning,
    apilevel,
    threadsafety,
)

from . import (
    ENTRYPOINT,
    ConnectionOptions,
    DatabaseOptions,
    StatementOptions,
    _db_kwargs,
    _driver_path,
)

# "named" is the closest PEP 249 style, but GoogleSQL requires `@name`, not `:name`.
paramstyle = "named"

#: PEP 249 constructor for BYTES values.
Binary = bytes

__all__ = [
    "connect",
    "Connection",
    "Cursor",
    "DatabaseOptions",
    "ConnectionOptions",
    "StatementOptions",
    "apilevel",
    "threadsafety",
    "paramstyle",
    "Date",
    "Time",
    "Timestamp",
    "DateFromTicks",
    "TimeFromTicks",
    "TimestampFromTicks",
    "Binary",
    "STRING",
    "BINARY",
    "NUMBER",
    "DATETIME",
    "ROWID",
    "Warning",
    "Error",
    "InterfaceError",
    "DatabaseError",
    "DataError",
    "OperationalError",
    "IntegrityError",
    "InternalError",
    "ProgrammingError",
    "NotSupportedError",
]


def connect(
    uri: str | None = None,
    db_kwargs: Mapping[str, str] | None = None,
    conn_kwargs: Mapping[str, str] | None = None,
    *,
    autocommit: bool = False,
) -> Connection:
    """Open a DBAPI 2.0 connection to a Spanner database.

    Parameters
    ----------
    uri:
        The database, such as ``spanner:///projects/p/instances/i/databases/d``.
        Overrides ``DatabaseOptions.URI`` in *db_kwargs*.
    db_kwargs:
        Database options keyed by :class:`DatabaseOptions` members or raw strings.
        Pass credentials here; inline credential JSON and access tokens are
        forbidden in URI parameters.
    conn_kwargs:
        Connection options keyed by :class:`ConnectionOptions` members.
    autocommit:
        ``False`` (the default) buffers writes until ``commit()``. Queries and
        writes cannot share a manual transaction. DDL always applies immediately.
        Overrides ``ConnectionOptions.AUTOCOMMIT`` in *conn_kwargs*.
    """
    conn_kwargs = dict(conn_kwargs or {})
    # The driver manager assumes a new connection starts in autocommit mode.
    conn_kwargs.pop(ConnectionOptions.AUTOCOMMIT, None)
    return adbc_driver_manager.dbapi.connect(
        driver=_driver_path(),
        entrypoint=ENTRYPOINT,
        db_kwargs=_db_kwargs(uri, db_kwargs),
        conn_kwargs=conn_kwargs,
        autocommit=autocommit,
    )
