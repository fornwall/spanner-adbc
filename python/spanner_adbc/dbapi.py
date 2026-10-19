"""DBAPI cursors and Arrow/DataFrame helpers for the Spanner ADBC driver.

    import spanner_adbc.dbapi as spanner
    with spanner.connect(
        db_kwargs={"uri": "spanner:///projects/p/instances/i/databases/d"}
    ) as conn:
        df = conn.cursor().execute("SELECT * FROM Singers").fetch_df()

Autocommit is off by default: call ``conn.commit()`` to apply buffered writes.
Closing a connection discards uncommitted writes. Use ``autocommit=True`` to
commit each DML statement immediately.
"""

from collections.abc import Mapping

import adbc_driver_manager.dbapi
from adbc_driver_manager import (
    DataError,
    DatabaseError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    Warning,
)

# PEP 249 type objects and constructors. Spanner has no TIME type, so `Time` values can't be bound.
from adbc_driver_manager.dbapi import (
    BINARY,
    DATETIME,
    NUMBER,
    ROWID,
    STRING,
    Date,
    DateFromTicks,
    Time,
    TimeFromTicks,
    Timestamp,
    TimestampFromTicks,
    apilevel,
    threadsafety,
)

from . import (
    ENTRYPOINT,
    ConnectionOptions,
    DatabaseOptions,
    StatementOptions,
    _driver_path,
)

# GoogleSQL marks parameters as `@name`; `?` is rejected. A mapping binds by name and a sequence
# binds in order of first appearance. "named" is the closest PEP 249 style, but its `:name`
# spelling is not accepted, so code that generates SQL must emit `@name` itself.
paramstyle = "named"

#: PEP 249 constructor for BYTES values.
Binary = bytes

__all__ = [
    "connect",
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
    db_kwargs: Mapping[str, str] | None = None,
    conn_kwargs: Mapping[str, str] | None = None,
    autocommit: bool = False,
) -> adbc_driver_manager.dbapi.Connection:
    """Open a DBAPI 2.0 connection to a Spanner database.

    Parameters
    ----------
    db_kwargs:
        Database options keyed by :class:`DatabaseOptions` members or raw strings.
        Include ``DatabaseOptions.URI``, for example
        ``spanner:///projects/p/instances/i/databases/d``. Pass credentials here;
        inline credential JSON and access tokens are forbidden in URI parameters.
    conn_kwargs:
        Connection-level options, keyed with the :class:`ConnectionOptions`
        constants (``adbc.connection.*`` / ``spanner.*``).
    autocommit:
        ``False`` (the default) buffers writes until ``commit()``. Queries and
        writes cannot share a manual transaction. DDL always applies immediately.
    """
    return adbc_driver_manager.dbapi.connect(
        driver=_driver_path(),
        entrypoint=ENTRYPOINT,
        db_kwargs=db_kwargs,
        conn_kwargs=conn_kwargs,
        autocommit=autocommit,
    )
