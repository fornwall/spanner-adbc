# Spanner dialect and capability overrides for https://github.com/adbc-drivers/validation.
import os
from pathlib import Path

from adbc_drivers_validation import model, quirks


# Cover variable-width, fixed-width, decimal and timestamp conversion in both
# dictionary and stream paths without multiplying every test by every type.
BIND_VARIANT_TYPES = {"string", "large_string", "binary", "int64", "decimal", "timestamp_us"}


class SpannerQuirks(model.DriverQuirks):
    name = "spanner"
    driver = "spanner_adbc"
    # Must match the driver's get_info values (src/info.rs).
    driver_name = "spanner-adbc"
    vendor_name = "Google Cloud Spanner"
    vendor_version = None  # Spanner exposes no server version
    short_version = "emulator"

    features = model.DriverFeatures(
        connection_get_table_schema=True,
        connection_get_statistics=True,
        connection_transactions=True,
        get_objects=True,
        get_objects_constraints_check=True,
        # The DDL hook below creates only the declared constraints each test expects.
        # Hidden rowid constraints are omitted from metadata; declared key order is
        # preserved, and non-FK constraint_column_usage is NULL.
        get_objects_constraints_foreign=True,
        get_objects_constraints_primary=True,
        statement_bind=True,
        statement_bulk_ingest=True,
        statement_bulk_ingest_schema=True,
        statement_execute_schema=True,
        statement_get_parameter_schema=True,
        statement_prepare=True,
        statement_rows_affected=True,
        # xdbc_column_def is also filled, but null for columns without a DEFAULT.
        supported_xdbc_fields=[
            "xdbc_type_name",
            "xdbc_nullable",
            "xdbc_is_nullable",
            "xdbc_is_autoincrement",
            "xdbc_is_generatedcolumn",
        ],
        # The driver reports the database id as the ADBC catalog; the default schema is
        # the unnamed "" of GoogleSQL INFORMATION_SCHEMA.
        current_catalog="adbc-test",
        current_schema="",
        secondary_schema="validation_schema",
    )

    # Database options are filled in by get_quirks() for the selected backend.
    setup = model.DriverSetup(
        database={"uri": model.FromEnv("ADBC_SPANNER_URI")},
    )

    @property
    def queries_paths(self) -> tuple[Path]:
        return (Path(__file__).parent.parent / "queries" / "spanner",)

    def bind_parameter(self, index: int) -> str:
        # Spanner has only named parameters; the driver binds column pN to @pN.
        return f"@p{index}"

    def query_override(self, context: str, default: str) -> str:
        if context == "TestConnection.test_get_table_schema_schema":
            return default.replace(
                "(spam INT, eggs VARCHAR)", "(spam INT64, eggs STRING(MAX))"
            )
        # Adapt the sample table to GoogleSQL type names and an explicit primary key.
        if context == "TestStatement.sample_table":
            return "CREATE TABLE `sample_table` (id INT64, value STRING(MAX)) PRIMARY KEY (id)"
        if context == "TestStatement.test_rows_affected.create_table":
            # No PRIMARY KEY: the test runs `UPDATE ... SET id = id + 1`, which Spanner
            # rejects on a key column. A keyless table is keyed on a hidden `rowid`.
            return default.replace("(id INT)", "(id INT64)")
        return super().query_override(context, default)

    def quote_one_identifier(self, identifier: str) -> str:
        return "`" + identifier.replace("`", "``") + "`"

    @property
    def sample_ddl_constraints(self) -> list[str]:
        # Parents precede children. Spanner has no UNIQUE table constraint.
        # The FK children declare no primary key because the suite expects exactly
        # one constraint per table; get_objects omits their hidden `rowid` key.
        return [
            "CREATE TABLE constraint_check (a INT64, b INT64,"
            " CONSTRAINT check_positive CHECK (a > 0),"
            " CONSTRAINT check_order CHECK (a < b))",
            "CREATE TABLE constraint_primary (a INT64, b INT64) PRIMARY KEY (a)",
            "CREATE TABLE constraint_primary_multi (a INT64, b INT64) PRIMARY KEY (b, a)",
            "CREATE TABLE constraint_primary_multi2 (a INT64, b INT64) PRIMARY KEY (a, b)",
            "CREATE TABLE constraint_foreign ("
            " a INT64, b INT64,"
            " CONSTRAINT fk_constraint_foreign FOREIGN KEY (b)"
            " REFERENCES constraint_primary (a)"
            ")",
            "CREATE TABLE constraint_foreign_multi ("
            " a INT64, b INT64, c INT64,"
            " CONSTRAINT fk_constraint_foreign_multi FOREIGN KEY (c, b)"
            " REFERENCES constraint_primary_multi2 (a, b)"
            ")",
        ]

    def split_statement(self, statement: str) -> list[str]:
        return quirks.split_statement(statement)

    def is_table_not_found(self, table_name, error: Exception) -> bool:
        text = str(error).lower()
        if "not found" in text:
            return table_name is None or table_name.lower() in text
        return False


def get_quirks(vendor_version: str | None = None) -> SpannerQuirks:
    q = SpannerQuirks()
    database = {"uri": model.FromEnv("ADBC_SPANNER_URI")}
    if endpoint := os.environ.get("SPANNER_OMNI_ENDPOINT"):
        if os.environ.get("SPANNER_EMULATOR_HOST"):
            raise ValueError("Configure only one of SPANNER_OMNI_ENDPOINT and SPANNER_EMULATOR_HOST")
        database.update({"spanner.instance_type": "omni", "spanner.endpoint": endpoint})
        q.short_version = "omni"
    elif os.environ.get("SPANNER_EMULATOR_HOST"):
        database["spanner.emulator"] = "true"
    if vendor_version is not None:
        q.short_version = vendor_version
    q.setup = model.DriverSetup(database=database)
    return q
