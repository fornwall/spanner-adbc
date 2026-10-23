// Runs the canonical Apache Arrow ADBC C++ validation suite against the
// spanner-adbc driver, loaded as a shared library through the ADBC driver
// manager. The driver library path and target Spanner database come from the
// environment (set by scripts/run-adbc-validation.sh):
//
//   ADBC_SPANNER_LIBRARY   path to the built cdylib (libspanner_adbc.so)
//   ADBC_SPANNER_URI       spanner:///projects/<p>/instances/<i>/databases/<d>
//   SPANNER_EMULATOR_HOST  (read by the driver itself) selects the emulator
//
// `SpannerQuirks` describes Spanner's capabilities to the suite so tests that do
// not apply to Spanner's model (temp tables, auto-increment primary keys, ...)
// self-skip rather than fail.

#include <cstdlib>
#include <cstring>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <arrow-adbc/adbc.h>
#include <gtest/gtest.h>

#include "validation/adbc_validation.h"
#include "validation/adbc_validation_util.h"

namespace {

std::string EnvOr(const char* name, const char* fallback) {
  const char* value = std::getenv(name);
  return value ? std::string(value) : std::string(fallback);
}

class SpannerQuirks : public adbc_validation::DriverQuirks {
 public:
  AdbcStatusCode SetupDatabase(struct AdbcDatabase* database,
                               struct AdbcError* error) const override {
    // The driver itself reads SPANNER_EMULATOR_HOST.
    RAISE_ADBC(AdbcDatabaseSetOption(database, "driver",
                                     EnvOr("ADBC_SPANNER_LIBRARY", "").c_str(), error));
    RAISE_ADBC(AdbcDatabaseSetOption(database, "entrypoint", "AdbcSpannerInit", error));
    RAISE_ADBC(AdbcDatabaseSetOption(database, "uri",
                                     EnvOr("ADBC_SPANNER_URI", "").c_str(), error));
    return ADBC_STATUS_OK;
  }

  // Spanner DDL runs through the admin API; the driver detects and routes it, so
  // an ordinary ExecuteQuery of a DROP/CREATE works here.
  AdbcStatusCode DropTable(struct AdbcConnection* connection, const std::string& name,
                           struct AdbcError* error) const override {
    return DropTable(connection, name, "", error);
  }

  AdbcStatusCode DropTable(struct AdbcConnection* connection, const std::string& name,
                           const std::string& db_schema,
                           struct AdbcError* error) const override {
    return RunIgnoringResult(connection,
                             "DROP TABLE IF EXISTS " + Qualified(name, db_schema), error);
  }

  // Spanner has named schemas (CREATE SCHEMA), so db-schema-scoped metadata can be tested.
  AdbcStatusCode EnsureDbSchema(struct AdbcConnection* connection,
                                const std::string& name,
                                struct AdbcError* error) const override {
    return RunIgnoringResult(connection, "CREATE SCHEMA IF NOT EXISTS `" + name + "`",
                             error);
  }

  // No `PRIMARY KEY` clause: Spanner keys such a table on a hidden `rowid`, so
  // its visible columns are just the two the suite expects.
  AdbcStatusCode CreateSampleTable(struct AdbcConnection* connection,
                                   const std::string& name,
                                   struct AdbcError* error) const override {
    return CreateSampleTable(connection, name, "", error);
  }

  AdbcStatusCode CreateSampleTable(struct AdbcConnection* connection,
                                   const std::string& name, const std::string& schema,
                                   struct AdbcError* error) const override {
    const std::string table = Qualified(name, schema);
    RAISE_ADBC(RunIgnoringResult(
        connection, "CREATE TABLE " + table + " (int64s INT64, strings STRING(MAX))",
        error));
    return RunIgnoringResult(
        connection,
        "INSERT INTO " + table +
            " (int64s, strings) VALUES (42, 'foo'), (-42, NULL), (NULL, '')",
        error);
  }

  std::optional<std::string> PrimaryKeyTableDdl(std::string_view name) const override {
    return "CREATE TABLE `" + std::string(name) + "` (id INT64) PRIMARY KEY (id)";
  }

  // The suite expects ordered auto-increment keys. Spanner's bit-reversed
  // sequence keys do not preserve insertion order, so this case is unsupported.
  std::optional<std::string> PrimaryKeyIngestTableDdl(std::string_view) const override {
    return std::nullopt;
  }

  std::optional<std::string> CompositePrimaryKeyTableDdl(
      std::string_view name) const override {
    return "CREATE TABLE `" + std::string(name) +
           "` (id_primary_col1 INT64, id_primary_col2 INT64) "
           "PRIMARY KEY (id_primary_col1, id_primary_col2)";
  }

  // A child table with a single-column FK (id_child_col3 -> parent_1.id) and a composite FK
  // ((id_child_col1, id_child_col2) -> parent_2.(id_primary_col1, id_primary_col2)); Spanner
  // supports table-level FOREIGN KEY constraints.
  std::optional<std::string> ForeignKeyChildTableDdl(
      std::string_view child_name, std::string_view parent_name_1,
      std::string_view parent_name_2) const override {
    return "CREATE TABLE `" + std::string(child_name) +
           "` (id_child_col1 INT64, id_child_col2 INT64, id_child_col3 INT64, "
           "CONSTRAINT fk_single FOREIGN KEY (id_child_col3) REFERENCES `" +
           std::string(parent_name_1) +
           "` (id), "
           "CONSTRAINT fk_composite FOREIGN KEY (id_child_col1, id_child_col2) "
           "REFERENCES `" +
           std::string(parent_name_2) +
           "` (id_primary_col1, id_primary_col2)) PRIMARY KEY (id_child_col1)";
  }

  // Spanner uses named query parameters (@name); the driver binds a column named
  // `pN` to @pN.
  std::string BindParameter(int index) const override {
    return "@p" + std::to_string(index);
  }

  // GoogleSQL uses backticks for identifiers and double quotes for strings.
  std::string QuoteIdentifier(std::string_view name) const override {
    return "`" + std::string(name) + "`";
  }

  // Adapt native type names, INSERT column lists and NULL ordering to GoogleSQL.
  // Readbacks with order-sensitive expectations need explicit ORDER BY clauses.
  // Pin each upstream query so a changed suite query requires reviewing its rewrite.
  std::string RewriteSql(std::string_view query_id,
                         std::string default_sql) const override {
    struct Rewrite {
      std::string_view expected_default;
      std::string_view sql;
    };
    static const std::unordered_map<std::string_view, Rewrite> kRewrites = {
        // GoogleSQL has no bare `FLOAT` type; its 64-bit float is `FLOAT64`.
        {"StatementTest::TestSqlQueryFloats::cast-1.5-as-float",
         {"SELECT CAST(1.5 AS FLOAT)", "SELECT CAST(1.5 AS FLOAT64)"}},
        {"StatementTest::TestSqlSchemaFloats::cast-1.5-as-float",
         {"SELECT CAST(1.5 AS FLOAT)", "SELECT CAST(1.5 AS FLOAT64)"}},
        // Ingest readbacks: drop NULLS FIRST/LAST, which are GoogleSQL's
        // defaults for ASC and DESC.
        {"StatementTest::TestSqlIngestTemporalType::select-bulk-ingest",
         {"SELECT * FROM `bulk_ingest` ORDER BY `col` ASC NULLS FIRST",
          "SELECT * FROM `bulk_ingest` ORDER BY `col` ASC"}},
        {"StatementTest::TestSqlIngestInterval::select-bulk-ingest",
         {"SELECT * FROM `bulk_ingest` ORDER BY `col` ASC NULLS FIRST",
          "SELECT * FROM `bulk_ingest` ORDER BY `col` ASC"}},
        {"StatementTest::TestSqlIngestMultipleConnections::select-bulk-ingest",
         {"SELECT * FROM `bulk_ingest` ORDER BY `int64s` DESC NULLS LAST",
          "SELECT * FROM `bulk_ingest` ORDER BY `int64s` DESC"}},
        {"StatementTest::TestSqlIngestSample::select-bulk-ingest",
         {"SELECT * FROM `bulk_ingest` ORDER BY int64s ASC NULLS FIRST",
          "SELECT * FROM `bulk_ingest` ORDER BY `int64s` ASC"}},
        // Append expects {42, -42, NULL}; explicit descending order reproduces this.
        // Other unordered readbacks compare rows without relying on insertion order.
        {"StatementTest::TestSqlIngestAppend::select-bulk-ingest",
         {"SELECT * FROM `bulk_ingest`",
          "SELECT * FROM `bulk_ingest` ORDER BY `int64s` DESC"}},
        // Add the INSERT column list and omit the implicit NULLS FIRST.
        {"StatementTest::TestSqlPrepareUpdate::insert-bulk-ingest",
         {"INSERT INTO `bulk_ingest` VALUES (@p0)",
          "INSERT INTO `bulk_ingest` (`int64s`) VALUES (@p0)"}},
        {"StatementTest::TestSqlPrepareUpdate::select-bulk-ingest",
         {"SELECT * FROM `bulk_ingest` ORDER BY `int64s` ASC NULLS FIRST",
          "SELECT * FROM `bulk_ingest` ORDER BY `int64s` ASC"}},
        {"StatementTest::TestSqlPrepareUpdateStream::insert-bulk-ingest",
         {"INSERT INTO `bulk_ingest` VALUES (@p0)",
          "INSERT INTO `bulk_ingest` (`ints`) VALUES (@p0)"}},
        {"StatementTest::TestSqlPrepareUpdateStream::select-bulk-ingest",
         {"SELECT * FROM `bulk_ingest` ORDER BY `ints` ASC NULLS FIRST",
          "SELECT * FROM `bulk_ingest` ORDER BY `ints` ASC"}},
        // Suite-internal DDL/DML: native type names and explicit INSERT column
        // lists. A table without a `PRIMARY KEY` clause is valid Spanner.
        {"StatementTest::TestSqlBind::create-table-bindtest",
         {"CREATE TABLE bindtest (col1 INTEGER, col2 TEXT)",
          "CREATE TABLE bindtest (col1 INT64, col2 STRING(MAX))"}},
        {"StatementTest::TestSqlBind::insert-bindtest",
         {"INSERT INTO bindtest VALUES (@p0, @p1)",
          "INSERT INTO bindtest (col1, col2) VALUES (@p0, @p1)"}},
        {"StatementTest::TestSqlBind::select-bindtest",
         {"SELECT * FROM bindtest ORDER BY col1 ASC NULLS FIRST",
          "SELECT * FROM bindtest ORDER BY col1 ASC"}},
        {"StatementTest::TestSqlQueryEmpty::create-table-queryempty",
         {"CREATE TABLE queryempty (FOO INT)", "CREATE TABLE queryempty (FOO INT64)"}},
        {"StatementTest::TestSqlQueryInsertRollback::create-table-rollbacktest",
         {"CREATE TABLE `rollbacktest` (a INT)",
          "CREATE TABLE `rollbacktest` (a INT64)"}},
        {"StatementTest::TestSqlQueryRowsAffectedDelete::create-table-delete-test",
         {"CREATE TABLE `delete_test` (foo INT)",
          "CREATE TABLE `delete_test` (foo INT64)"}},
        {"StatementTest::TestSqlQueryRowsAffectedDeleteStream::create-table-delete-test",
         {"CREATE TABLE `delete_test` (foo INT)",
          "CREATE TABLE `delete_test` (foo INT64)"}},
    };

    // TestSqlIngestType query ids end with the ingested Arrow type. GoogleSQL
    // cannot ORDER BY an array, so list columns order by their first element,
    // which matches the expected NULL-first ascending data.
    constexpr std::string_view kIngestTypePrefix =
        "StatementTest::TestSqlIngestType::select-bulk-ingest::";
    if (query_id.substr(0, kIngestTypePrefix.size()) == kIngestTypePrefix) {
      EXPECT_EQ(default_sql, "SELECT * FROM `bulk_ingest` ORDER BY `col` ASC NULLS FIRST")
          << "upstream default SQL for " << query_id << " changed; revisit the rewrite";
      if (query_id.substr(kIngestTypePrefix.size()) == "list") {
        return "SELECT * FROM `bulk_ingest` ORDER BY `col`[SAFE_OFFSET(0)] ASC";
      }
      return "SELECT * FROM `bulk_ingest` ORDER BY `col` ASC";
    }

    auto it = kRewrites.find(query_id);
    if (it == kRewrites.end()) return default_sql;
    EXPECT_EQ(default_sql, it->second.expected_default)
        << "upstream default SQL for " << query_id << " changed; revisit the rewrite";
    return std::string(it->second.sql);
  }

  // Ingested Arrow types read back as the canonical type of their Spanner
  // column: INT64, FLOAT32 (for half floats), STRING(MAX) or BYTES(MAX). See
  // `bind::spanner_column_type` for ingest and `src/conversion.rs` for reads.
  // The base class's SchemaField overload applies this to nested types. UInt64
  // is absent because ingest rejects it.
  ArrowType IngestSelectRoundTripType(ArrowType ingest_type) const override {
    switch (ingest_type) {
      case NANOARROW_TYPE_INT8:
      case NANOARROW_TYPE_INT16:
      case NANOARROW_TYPE_INT32:
      case NANOARROW_TYPE_UINT8:
      case NANOARROW_TYPE_UINT16:
      case NANOARROW_TYPE_UINT32:
        return NANOARROW_TYPE_INT64;
      case NANOARROW_TYPE_HALF_FLOAT:
        return NANOARROW_TYPE_FLOAT;
      case NANOARROW_TYPE_LARGE_STRING:
      case NANOARROW_TYPE_STRING_VIEW:
        return NANOARROW_TYPE_STRING;
      case NANOARROW_TYPE_LARGE_BINARY:
      case NANOARROW_TYPE_BINARY_VIEW:
      case NANOARROW_TYPE_FIXED_SIZE_BINARY:
        return NANOARROW_TYPE_BINARY;
      default:
        return ingest_type;
    }
  }

  // Created tables have no `PRIMARY KEY` clause, so their columns are just the
  // ingested ones.
  bool supports_bulk_ingest(const char* mode) const override {
    return std::strcmp(mode, ADBC_INGEST_OPTION_MODE_APPEND) == 0 ||
           std::strcmp(mode, ADBC_INGEST_OPTION_MODE_CREATE) == 0 ||
           std::strcmp(mode, ADBC_INGEST_OPTION_MODE_CREATE_APPEND) == 0 ||
           std::strcmp(mode, ADBC_INGEST_OPTION_MODE_REPLACE) == 0;
  }

  bool supports_execute_schema() const override { return true; }
  bool supports_get_sql_info() const override { return true; }
  bool supports_get_objects() const override { return true; }
  bool supports_partitioned_data() const override { return true; }
  bool supports_statistics() const override { return true; }
  bool supports_cancel() const override { return true; }
  bool supports_dynamic_parameter_binding() const override { return true; }
  // Append probes INFORMATION_SCHEMA on failure to distinguish missing tables
  // (NotFound) from incompatible schemas (AlreadyExists).
  bool supports_error_on_incompatible_schema() const override { return true; }
  bool supports_concurrent_statements() const override { return true; }
  bool supports_transactions() const override { return true; }
  // DDL applies immediately through the admin API and cannot be rolled back.
  // This also skips the suite transaction case, which expects reads to see
  // uncommitted writes; this driver buffers writes until commit.
  bool ddl_implicit_commit_txn() const override { return true; }
  // Spanner's catalog level is the database itself, and its default schema is the
  // unnamed "" of INFORMATION_SCHEMA, so the connection reports both rather than
  // NOT_FOUND.
  bool supports_metadata_current_catalog() const override { return true; }
  bool supports_metadata_current_db_schema() const override { return true; }
  bool supports_ingest_view_types() const override { return true; }
  bool supports_bulk_ingest_catalog() const override { return true; }
  bool supports_bulk_ingest_db_schema() const override { return true; }

  // The ADBC catalog is the database id, excluding URI query parameters.
  std::string catalog() const override {
    const std::string uri = EnvOr("ADBC_SPANNER_URI", "");
    const std::string path = uri.substr(0, uri.find_first_of("?#"));
    const auto slash = path.find_last_of('/');
    return slash == std::string::npos ? path : path.substr(slash + 1);
  }
  std::string db_schema() const override { return ""; }

 private:
  // Backtick-quote a table name, optionally qualified by a named schema.
  static std::string Qualified(const std::string& name, const std::string& schema) {
    if (schema.empty()) return "`" + name + "`";
    return "`" + schema + "`.`" + name + "`";
  }

  // Execute a statement purely for its side effect (DDL/DML), discarding any result.
  static AdbcStatusCode RunIgnoringResult(struct AdbcConnection* connection,
                                          const std::string& sql,
                                          struct AdbcError* error) {
    adbc_validation::Handle<struct AdbcStatement> statement;
    RAISE_ADBC(AdbcStatementNew(connection, &statement.value, error));
    RAISE_ADBC(AdbcStatementSetSqlQuery(&statement.value, sql.c_str(), error));
    RAISE_ADBC(AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, error));
    return AdbcStatementRelease(&statement.value, error);
  }
};

class SpannerDatabaseTest : public ::testing::Test, public adbc_validation::DatabaseTest {
 public:
  const adbc_validation::DriverQuirks* quirks() const override { return &quirks_; }
  void SetUp() override { ASSERT_NO_FATAL_FAILURE(SetUpTest()); }
  void TearDown() override { ASSERT_NO_FATAL_FAILURE(TearDownTest()); }

 protected:
  SpannerQuirks quirks_;
};
ADBCV_TEST_DATABASE(SpannerDatabaseTest)

class SpannerConnectionTest : public ::testing::Test,
                              public adbc_validation::ConnectionTest {
 public:
  const adbc_validation::DriverQuirks* quirks() const override { return &quirks_; }
  void SetUp() override { ASSERT_NO_FATAL_FAILURE(SetUpTest()); }
  void TearDown() override { ASSERT_NO_FATAL_FAILURE(TearDownTest()); }

 protected:
  void ExecuteSql(const std::string& sql) {
    adbc_validation::Handle<struct AdbcStatement> stmt;
    ASSERT_THAT(AdbcStatementNew(&connection, &stmt.value, &error),
                adbc_validation::IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementSetSqlQuery(&stmt.value, sql.c_str(), &error),
                adbc_validation::IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementExecuteQuery(&stmt.value, nullptr, nullptr, &error),
                adbc_validation::IsOkStatus(&error));
  }

  void ExpectIds(struct AdbcConnection* observer, const std::string& table,
                 const std::vector<int64_t>& expected) {
    adbc_validation::Handle<struct AdbcStatement> stmt;
    adbc_validation::StreamReader reader;
    const std::string sql = "SELECT id FROM `" + table + "` ORDER BY id";
    ASSERT_THAT(AdbcStatementNew(observer, &stmt.value, &error),
                adbc_validation::IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementSetSqlQuery(&stmt.value, sql.c_str(), &error),
                adbc_validation::IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementExecuteQuery(&stmt.value, &reader.stream.value,
                                          &reader.rows_affected, &error),
                adbc_validation::IsOkStatus(&error));
    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
    ASSERT_NO_FATAL_FAILURE(adbc_validation::CompareSchema(
        &reader.schema.value, {{"id", NANOARROW_TYPE_INT64}}));
    std::vector<int64_t> actual;
    while (true) {
      ASSERT_NO_FATAL_FAILURE(reader.Next());
      if (!reader.array->release) break;
      auto* values = reader.array_view->children[0];
      for (int64_t row = 0; row < values->length; ++row) {
        ASSERT_FALSE(ArrowArrayViewIsNull(values, row));
        actual.push_back(ArrowArrayViewGetIntUnsafe(values, row));
      }
    }
    EXPECT_EQ(actual, expected);
  }

  SpannerQuirks quirks_;
};
ADBCV_TEST_CONNECTION(SpannerConnectionTest)

TEST_F(SpannerConnectionTest, BufferedWritesCommitAndEnableAutocommit) {
  ASSERT_THAT(AdbcConnectionNew(&connection, &error),
              adbc_validation::IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionInit(&connection, &database, &error),
              adbc_validation::IsOkStatus(&error));
  ASSERT_THAT(quirks()->DropTable(&connection, "buffered_commit", &error),
              adbc_validation::IsOkStatus(&error));
  ASSERT_NO_FATAL_FAILURE(ExecuteSql("CREATE TABLE buffered_commit (id INT64)"));

  adbc_validation::Handle<struct AdbcConnection> observer;
  ASSERT_THAT(AdbcConnectionNew(&observer.value, &error),
              adbc_validation::IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionInit(&observer.value, &database, &error),
              adbc_validation::IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionSetOption(&connection, ADBC_CONNECTION_OPTION_AUTOCOMMIT,
                                      ADBC_OPTION_VALUE_DISABLED, &error),
              adbc_validation::IsOkStatus(&error));

  ASSERT_NO_FATAL_FAILURE(ExecuteSql("INSERT INTO buffered_commit (id) VALUES (1)"));
  ASSERT_NO_FATAL_FAILURE(ExecuteSql("INSERT INTO buffered_commit (id) VALUES (2)"));
  ASSERT_NO_FATAL_FAILURE(ExpectIds(&observer.value, "buffered_commit", {}));
  ASSERT_THAT(AdbcConnectionCommit(&connection, &error),
              adbc_validation::IsOkStatus(&error));
  ASSERT_NO_FATAL_FAILURE(ExpectIds(&observer.value, "buffered_commit", {1, 2}));

  ASSERT_NO_FATAL_FAILURE(ExecuteSql("INSERT INTO buffered_commit (id) VALUES (3)"));
  ASSERT_NO_FATAL_FAILURE(ExpectIds(&observer.value, "buffered_commit", {1, 2}));
  ASSERT_THAT(AdbcConnectionSetOption(&connection, ADBC_CONNECTION_OPTION_AUTOCOMMIT,
                                      ADBC_OPTION_VALUE_ENABLED, &error),
              adbc_validation::IsOkStatus(&error));
  ASSERT_NO_FATAL_FAILURE(ExpectIds(&observer.value, "buffered_commit", {1, 2, 3}));
  ASSERT_THAT(quirks()->DropTable(&connection, "buffered_commit", &error),
              adbc_validation::IsOkStatus(&error));
}

TEST_F(SpannerConnectionTest, RollbackDiscardsWritesButKeepsDdl) {
  ASSERT_THAT(AdbcConnectionNew(&connection, &error),
              adbc_validation::IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionInit(&connection, &database, &error),
              adbc_validation::IsOkStatus(&error));
  ASSERT_THAT(quirks()->DropTable(&connection, "buffered_rollback", &error),
              adbc_validation::IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionSetOption(&connection, ADBC_CONNECTION_OPTION_AUTOCOMMIT,
                                      ADBC_OPTION_VALUE_DISABLED, &error),
              adbc_validation::IsOkStatus(&error));
  ASSERT_NO_FATAL_FAILURE(ExecuteSql("CREATE TABLE buffered_rollback (id INT64)"));
  ASSERT_NO_FATAL_FAILURE(ExecuteSql("INSERT INTO buffered_rollback (id) VALUES (1)"));
  ASSERT_NO_FATAL_FAILURE(
      ExecuteSql("ALTER TABLE buffered_rollback ADD COLUMN label STRING(MAX)"));

  adbc_validation::Handle<struct AdbcConnection> observer;
  ASSERT_THAT(AdbcConnectionNew(&observer.value, &error),
              adbc_validation::IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionInit(&observer.value, &database, &error),
              adbc_validation::IsOkStatus(&error));
  ASSERT_NO_FATAL_FAILURE(ExpectIds(&observer.value, "buffered_rollback", {}));
  ASSERT_THAT(AdbcConnectionRollback(&connection, &error),
              adbc_validation::IsOkStatus(&error));
  ASSERT_NO_FATAL_FAILURE(ExpectIds(&observer.value, "buffered_rollback", {}));
  ASSERT_THAT(quirks()->DropTable(&connection, "buffered_rollback", &error),
              adbc_validation::IsOkStatus(&error));
}

class SpannerStatementTest : public ::testing::Test,
                             public adbc_validation::StatementTest {
 public:
  const adbc_validation::DriverQuirks* quirks() const override { return &quirks_; }
  void SetUp() override { ASSERT_NO_FATAL_FAILURE(SetUpTest()); }
  void TearDown() override { ASSERT_NO_FATAL_FAILURE(TearDownTest()); }

 protected:
  // The base class fails this check by default. Only TIMESTAMP ingest gets
  // here, since Duration ingest is unsupported. Timestamps of any unit read
  // back as Timestamp(Nanosecond, "UTC"), so the suite's {NULL, -42, 0, 42}
  // come back scaled to nanoseconds; the timezone is only metadata.
  void ValidateIngestedTemporalData(struct ArrowArrayView* values, ArrowType type,
                                    enum ArrowTimeUnit unit,
                                    const char* /*timezone*/) override {
    ASSERT_EQ(NANOARROW_TYPE_TIMESTAMP, type)
        << "unexpected temporal ingest type reached the readback";
    int64_t factor = 1;
    switch (unit) {
      case NANOARROW_TIME_UNIT_SECOND:
        factor = 1000000000;
        break;
      case NANOARROW_TIME_UNIT_MILLI:
        factor = 1000000;
        break;
      case NANOARROW_TIME_UNIT_MICRO:
        factor = 1000;
        break;
      case NANOARROW_TIME_UNIT_NANO:
        factor = 1;
        break;
    }
    const std::vector<std::optional<int64_t>> expected{std::nullopt, -42 * factor, 0,
                                                       42 * factor};
    ASSERT_NO_FATAL_FAILURE(adbc_validation::CompareArray<int64_t>(values, expected));
  }

  SpannerQuirks quirks_;
};
ADBCV_TEST_STATEMENT(SpannerStatementTest)

}  // namespace
