#include "queen/routes/route_registry.hpp"
#include "queen/routes/route_context.hpp"
#include "queen/routes/route_helpers.hpp"
#include "queen/config.hpp"
#include "threadpool.hpp"
#include <libpq-fe.h>
#include <spdlog/spdlog.h>
#include <atomic>
#include <mutex>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <sys/wait.h>

namespace queen {
namespace routes {

// URL-encode a string component for use in postgresql:// URIs
static std::string url_encode(const std::string& s) {
    std::string result;
    result.reserve(s.size());
    for (unsigned char c : s) {
        if (isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
            result += static_cast<char>(c);
        } else {
            char hex[4];
            snprintf(hex, sizeof(hex), "%%%02X", c);
            result += hex;
        }
    }
    return result;
}

static std::string build_pg_uri(const std::string& user, const std::string& password,
                                const std::string& host, const std::string& port,
                                const std::string& database, bool use_ssl) {
    std::string uri = "postgresql://" + url_encode(user) + ":" + url_encode(password) +
                      "@" + host + ":" + port + "/" + url_encode(database);
    if (use_ssl) {
        uri += "?sslmode=require";
    } else {
        uri += "?sslmode=disable";
    }
    return uri;
}

static std::string build_connstr(const std::string& user, const std::string& password,
                                 const std::string& host, const std::string& port,
                                 const std::string& database, bool use_ssl) {
    std::string conn = "host=" + host + " port=" + port + " dbname=" + database +
                       " user=" + user + " password=" + password;
    conn += use_ssl ? " sslmode=require" : " sslmode=disable";
    conn += " connect_timeout=10";
    return conn;
}

// ============================================================================
// Migration State (singleton, thread-safe)
// ============================================================================

enum class MigrationStatus {
    IDLE = 0,
    DUMPING = 1,
    RESTORING = 2,
    COMPLETE = 3,
    ERROR = 4
};

struct MigrationState {
    std::atomic<int> status{static_cast<int>(MigrationStatus::IDLE)};
    std::mutex mtx;
    std::string error_message;
    std::string current_step;
    std::string dump_output;
    std::string restore_output;
    std::chrono::steady_clock::time_point start_time;
    double elapsed_seconds{0};

    void reset() {
        std::lock_guard<std::mutex> lock(mtx);
        status.store(static_cast<int>(MigrationStatus::IDLE));
        error_message.clear();
        current_step.clear();
        dump_output.clear();
        restore_output.clear();
        elapsed_seconds = 0;
    }

    void set_error(const std::string& msg) {
        std::lock_guard<std::mutex> lock(mtx);
        error_message = msg;
        status.store(static_cast<int>(MigrationStatus::ERROR));
        auto now = std::chrono::steady_clock::now();
        elapsed_seconds = std::chrono::duration<double>(now - start_time).count();
    }

    void set_step(const std::string& step) {
        std::lock_guard<std::mutex> lock(mtx);
        current_step = step;
    }

    nlohmann::json to_json() {
        std::lock_guard<std::mutex> lock(mtx);
        auto now = std::chrono::steady_clock::now();
        int s = status.load();
        double elapsed = (s != static_cast<int>(MigrationStatus::IDLE))
            ? std::chrono::duration<double>(now - start_time).count()
            : 0;
        if (s == static_cast<int>(MigrationStatus::COMPLETE) ||
            s == static_cast<int>(MigrationStatus::ERROR)) {
            elapsed = elapsed_seconds;
        }

        std::string status_str;
        switch (static_cast<MigrationStatus>(s)) {
            case MigrationStatus::IDLE:      status_str = "idle"; break;
            case MigrationStatus::DUMPING:    status_str = "dumping"; break;
            case MigrationStatus::RESTORING:  status_str = "restoring"; break;
            case MigrationStatus::COMPLETE:   status_str = "complete"; break;
            case MigrationStatus::ERROR:      status_str = "error"; break;
        }

        nlohmann::json j = {
            {"status", status_str},
            {"currentStep", current_step},
            {"elapsedSeconds", std::round(elapsed * 10) / 10},
        };
        if (!error_message.empty()) {
            j["error"] = error_message;
        }
        if (!dump_output.empty()) {
            j["dumpOutput"] = dump_output;
        }
        if (!restore_output.empty()) {
            j["restoreOutput"] = restore_output;
        }
        return j;
    }
};

static MigrationState g_migration_state;

// Run a shell command, capture stderr+stdout, return exit code
static int run_command(const std::string& cmd, std::string& output) {
    std::string full_cmd = cmd + " 2>&1";
    FILE* pipe = popen(full_cmd.c_str(), "r");
    if (!pipe) return -1;

    char buffer[4096];
    while (fgets(buffer, sizeof(buffer), pipe)) {
        output += buffer;
    }

    int status = pclose(pipe);
    if (WIFEXITED(status)) {
        return WEXITSTATUS(status);
    }
    return -1;
}

// ============================================================================
// Route Handlers
// ============================================================================

void setup_migration_routes(uWS::App* app, const RouteContext& ctx) {

    // ========================================================================
    // POST /api/v1/migration/test-connection
    // Test connectivity to a target PostgreSQL database
    // ========================================================================
    app->post("/api/v1/migration/test-connection", [ctx](auto* res, auto* req) {
        REQUIRE_AUTH(res, req, ctx, auth::AccessLevel::ADMIN);

        read_json_body(res,
            [res, ctx](const nlohmann::json& body) {
                try {
                    std::string host = body.value("host", "");
                    std::string port = body.value("port", "5432");
                    std::string database = body.value("database", "");
                    std::string user = body.value("user", "");
                    std::string password = body.value("password", "");
                    bool use_ssl = body.value("ssl", false);

                    if (host.empty() || database.empty() || user.empty()) {
                        send_error_response(res, "host, database, and user are required", 400);
                        return;
                    }

                    std::string conn_str = build_connstr(user, password, host, port, database, use_ssl);

                    auto* loop = uWS::Loop::get();
                    auto* response = res;

                    // Must detach response to keep it alive across the async boundary
                    res->onAborted([]() {});

                    ctx.db_thread_pool->push([response, loop, conn_str]() {
                        nlohmann::json result;
                        PGconn* conn = PQconnectdb(conn_str.c_str());

                        if (PQstatus(conn) == CONNECTION_OK) {
                            // Query server version
                            PGresult* ver_res = PQexec(conn, "SELECT version()");
                            std::string version;
                            if (PQresultStatus(ver_res) == PGRES_TUPLES_OK && PQntuples(ver_res) > 0) {
                                version = PQgetvalue(ver_res, 0, 0);
                            }
                            PQclear(ver_res);

                            result = {
                                {"success", true},
                                {"message", "Connection successful"},
                                {"version", version}
                            };
                        } else {
                            std::string err = PQerrorMessage(conn);
                            result = {
                                {"success", false},
                                {"message", "Connection failed"},
                                {"error", err}
                            };
                        }
                        PQfinish(conn);

                        loop->defer([response, result]() {
                            send_json_response(response, result);
                        });
                    });

                } catch (const std::exception& e) {
                    send_error_response(res, e.what(), 500);
                }
            },
            [res](const std::string& error) {
                send_error_response(res, error, 400);
            }
        );
    });

    // ========================================================================
    // POST /api/v1/migration/start
    // Begin database migration (pg_dump source -> pg_restore target)
    // ========================================================================
    app->post("/api/v1/migration/start", [ctx](auto* res, auto* req) {
        REQUIRE_AUTH(res, req, ctx, auth::AccessLevel::ADMIN);

        read_json_body(res,
            [res, ctx](const nlohmann::json& body) {
                try {
                    // Check not already running
                    int current = g_migration_state.status.load();
                    if (current == static_cast<int>(MigrationStatus::DUMPING) ||
                        current == static_cast<int>(MigrationStatus::RESTORING)) {
                        send_error_response(res, "Migration already in progress", 409);
                        return;
                    }

                    // Parse target DB config
                    std::string target_host = body.value("host", "");
                    std::string target_port = body.value("port", "5432");
                    std::string target_db = body.value("database", "");
                    std::string target_user = body.value("user", "");
                    std::string target_password = body.value("password", "");
                    bool target_ssl = body.value("ssl", false);
                    std::string schema = body.value("schema", "queen");

                    if (target_host.empty() || target_db.empty() || target_user.empty()) {
                        send_error_response(res, "target host, database, and user are required", 400);
                        return;
                    }

                    // Parse optional table data exclusions.
                    // --exclude-table-data keeps the CREATE TABLE DDL (so FK constraints
                    // are always satisfied) but skips the row data for selected tables.
                    // Only a fixed whitelist of known table names is accepted to prevent
                    // shell injection via the --exclude-table-data flag.
                    static const std::vector<std::string> SKIPPABLE_TABLES = {
                        "messages", "dead_letter_queue",
                        "message_traces", "message_trace_names",
                        "messages_consumed",
                        "stats", "stats_history", "system_metrics",
                        "worker_metrics", "worker_metrics_summary",
                        "queue_lag_metrics", "retention_history"
                    };
                    std::string exclude_flags;
                    if (body.contains("excludeTableData") && body["excludeTableData"].is_array()) {
                        for (const auto& t : body["excludeTableData"]) {
                            if (!t.is_string()) continue;
                            std::string tbl = t.get<std::string>();
                            if (std::find(SKIPPABLE_TABLES.begin(), SKIPPABLE_TABLES.end(), tbl)
                                    != SKIPPABLE_TABLES.end()) {
                                exclude_flags += " --exclude-table-data=" + schema + "." + tbl;
                                spdlog::info("[Migration] Skipping data for table: {}.{}", schema, tbl);
                            }
                        }
                    }

                    // Build source URI from server config
                    const auto& src = ctx.config.database;
                    std::string source_uri = build_pg_uri(
                        src.user, src.password, src.host, src.port, src.database, src.use_ssl);

                    std::string target_uri = build_pg_uri(
                        target_user, target_password, target_host, target_port, target_db, target_ssl);

                    // Reset state and mark as started
                    g_migration_state.reset();
                    g_migration_state.status.store(static_cast<int>(MigrationStatus::DUMPING));
                    g_migration_state.start_time = std::chrono::steady_clock::now();
                    g_migration_state.set_step("Preparing target schema...");

                    spdlog::info("[Migration] Starting migration to {}:{}/{}", target_host, target_port, target_db);

                    // Return immediately, migration runs in background
                    nlohmann::json response = {
                        {"status", "started"},
                        {"message", "Migration started. Poll /api/v1/migration/status for progress."}
                    };
                    send_json_response(res, response, 202);

                    // Build a connstr for the libpq pre-create step
                    std::string target_connstr = build_connstr(
                        target_user, target_password, target_host, target_port, target_db, target_ssl);

                    // Run migration in background thread
                    ctx.db_thread_pool->push([source_uri, target_uri, target_connstr, schema, exclude_flags, target_host, target_port, target_db]() {

                        // Step 1: Prepare target schema via libpq before streaming starts
                        g_migration_state.set_step("Preparing target database (creating schema)...");
                        {
                            PGconn* target_conn = PQconnectdb(target_connstr.c_str());
                            if (PQstatus(target_conn) != CONNECTION_OK) {
                                std::string err = PQerrorMessage(target_conn);
                                PQfinish(target_conn);
                                spdlog::error("[Migration] Cannot connect to target for schema prep: {}", err);
                                g_migration_state.set_error("Cannot connect to target: " + err);
                                return;
                            }

                            std::string create_sql = "DROP SCHEMA IF EXISTS " + schema + " CASCADE; "
                                                     "CREATE SCHEMA " + schema + ";";
                            PGresult* r = PQexec(target_conn, create_sql.c_str());
                            if (PQresultStatus(r) != PGRES_COMMAND_OK) {
                                std::string err = PQerrorMessage(target_conn);
                                PQclear(r);
                                PQfinish(target_conn);
                                spdlog::error("[Migration] Failed to create schema on target: {}", err);
                                g_migration_state.set_error("Failed to create schema on target: " + err);
                                return;
                            }
                            PQclear(r);
                            PQfinish(target_conn);
                            spdlog::info("[Migration] Target schema '{}' ready, starting stream", schema);
                        }

                        // Step 2: Stream pg_dump directly into pg_restore via kernel pipe.
                        //
                        // No temp file is written — data flows through a pipe buffer in memory.
                        // This avoids the /tmp disk space requirement (a compressed dump of 40GB
                        // source data can reach 8-13GB, overflowing container ephemeral storage).
                        //
                        // -Z 0 (no compression): compression wastes CPU when data never touches
                        // disk. Raw throughput through the pipe is faster without it.
                        //
                        // set -o pipefail: bash exits non-zero if pg_dump fails, even though
                        // pg_restore is the last command in the pipe.
                        //
                        // --exit-on-error: pg_restore stops on first error instead of continuing
                        // with partial data. The schema is always dropped+recreated before this
                        // step, so a failed migration is always safely retried via Reset.
                        g_migration_state.status.store(static_cast<int>(MigrationStatus::RESTORING));
                        g_migration_state.set_step("Streaming: pg_dump | pg_restore" +
                            std::string(exclude_flags.empty() ? "" : " (partial — some table data skipped)") +
                            "...");

                        std::string pipe_cmd = "bash -c 'set -o pipefail; "
                            "pg_dump --format=custom -Z 0"
                            " --schema=" + schema +
                            " --no-owner --no-privileges"
                            + exclude_flags +
                            " --dbname=\"" + source_uri + "\""
                            " | pg_restore"
                            " --no-owner --no-privileges --exit-on-error"
                            " --schema=" + schema +
                            " --dbname=\"" + target_uri + "\"'";

                        spdlog::info("[Migration] Streaming pg_dump | pg_restore to {}:{}/{} exclude_flags='{}'",
                            target_host, target_port, target_db, exclude_flags);

                        std::string pipe_output;
                        int pipe_exit = run_command(pipe_cmd, pipe_output);

                        {
                            std::lock_guard<std::mutex> lock(g_migration_state.mtx);
                            g_migration_state.restore_output = pipe_output;
                        }

                        if (pipe_exit != 0) {
                            spdlog::error("[Migration] Stream failed (exit {}): {}", pipe_exit, pipe_output);
                            g_migration_state.set_error("Migration failed (exit " +
                                std::to_string(pipe_exit) + "): " + pipe_output);
                            return;
                        }

                        // Done
                        {
                            std::lock_guard<std::mutex> lock(g_migration_state.mtx);
                            g_migration_state.current_step = "Migration complete";
                            auto now = std::chrono::steady_clock::now();
                            g_migration_state.elapsed_seconds =
                                std::chrono::duration<double>(now - g_migration_state.start_time).count();
                        }
                        g_migration_state.status.store(static_cast<int>(MigrationStatus::COMPLETE));

                        spdlog::info("[Migration] Migration to {}:{}/{} completed successfully",
                            target_host, target_port, target_db);
                    });

                } catch (const std::exception& e) {
                    send_error_response(res, e.what(), 500);
                }
            },
            [res](const std::string& error) {
                send_error_response(res, error, 400);
            }
        );
    });

    // ========================================================================
    // GET /api/v1/migration/status
    // Poll migration progress
    // ========================================================================
    app->get("/api/v1/migration/status", [ctx](auto* res, auto* req) {
        REQUIRE_AUTH(res, req, ctx, auth::AccessLevel::ADMIN);

        try {
            send_json_response(res, g_migration_state.to_json());
        } catch (const std::exception& e) {
            send_error_response(res, e.what(), 500);
        }
    });

    // ========================================================================
    // POST /api/v1/migration/validate
    // Compare row counts between source and target databases
    // ========================================================================
    app->post("/api/v1/migration/validate", [ctx](auto* res, auto* req) {
        REQUIRE_AUTH(res, req, ctx, auth::AccessLevel::ADMIN);

        read_json_body(res,
            [res, ctx](const nlohmann::json& body) {
                try {
                    std::string target_host = body.value("host", "");
                    std::string target_port = body.value("port", "5432");
                    std::string target_db = body.value("database", "");
                    std::string target_user = body.value("user", "");
                    std::string target_password = body.value("password", "");
                    bool target_ssl = body.value("ssl", false);

                    if (target_host.empty() || target_db.empty() || target_user.empty()) {
                        send_error_response(res, "target host, database, and user are required", 400);
                        return;
                    }

                    std::string target_connstr = build_connstr(
                        target_user, target_password, target_host, target_port, target_db, target_ssl);

                    auto* loop = uWS::Loop::get();
                    auto* response = res;
                    res->onAborted([]() {});

                    const auto& src_cfg = ctx.config.database;
                    std::string source_connstr = build_connstr(
                        src_cfg.user, src_cfg.password, src_cfg.host, src_cfg.port,
                        src_cfg.database, src_cfg.use_ssl);

                    ctx.db_thread_pool->push([response, loop, source_connstr, target_connstr]() {
                        static const std::vector<std::string> tables = {
                            "queen.queues",
                            "queen.partitions",
                            "queen.messages",
                            "queen.partition_consumers",
                            "queen.consumer_groups_metadata",
                            "queen.messages_consumed",
                            "queen.dead_letter_queue",
                            "queen.retention_history",
                            "queen.message_traces",
                            "queen.message_trace_names",
                            "queen.system_state",
                            "queen.partition_lookup",
                            "queen.stats",
                            "queen.stats_history",
                            "queen.system_metrics"
                        };

                        nlohmann::json result;
                        result["tables"] = nlohmann::json::array();
                        bool all_match = true;

                        // Connect to source
                        PGconn* source_conn_raw = PQconnectdb(source_connstr.c_str());
                        if (PQstatus(source_conn_raw) != CONNECTION_OK) {
                            std::string err = PQerrorMessage(source_conn_raw);
                            PQfinish(source_conn_raw);
                            nlohmann::json err_result = {
                                {"success", false},
                                {"error", "Failed to connect to source: " + err}
                            };
                            loop->defer([response, err_result]() {
                                send_json_response(response, err_result, 500);
                            });
                            return;
                        }

                        // Connect to target
                        PGconn* target_conn = PQconnectdb(target_connstr.c_str());
                        if (PQstatus(target_conn) != CONNECTION_OK) {
                            std::string err = PQerrorMessage(target_conn);
                            PQfinish(target_conn);
                            PQfinish(source_conn_raw);
                            nlohmann::json err_result = {
                                {"success", false},
                                {"error", "Failed to connect to target: " + err}
                            };
                            loop->defer([response, err_result]() {
                                send_json_response(response, err_result, 500);
                            });
                            return;
                        }

                        for (const auto& table : tables) {
                            std::string count_sql = "SELECT count(*) FROM " + table;

                            // Source count
                            long long source_count = -1;
                            PGresult* src_res = PQexec(source_conn_raw, count_sql.c_str());
                            if (PQresultStatus(src_res) == PGRES_TUPLES_OK && PQntuples(src_res) > 0) {
                                source_count = std::atoll(PQgetvalue(src_res, 0, 0));
                            }
                            PQclear(src_res);

                            // Target count
                            long long target_count = -1;
                            PGresult* tgt_res = PQexec(target_conn, count_sql.c_str());
                            if (PQresultStatus(tgt_res) == PGRES_TUPLES_OK && PQntuples(tgt_res) > 0) {
                                target_count = std::atoll(PQgetvalue(tgt_res, 0, 0));
                            }
                            PQclear(tgt_res);

                            bool match = (source_count == target_count);
                            if (!match) all_match = false;

                            std::string short_name = table;
                            auto dot = table.find('.');
                            if (dot != std::string::npos) short_name = table.substr(dot + 1);

                            result["tables"].push_back({
                                {"table", short_name},
                                {"sourceCount", source_count},
                                {"targetCount", target_count},
                                {"match", match}
                            });
                        }

                        PQfinish(source_conn_raw);
                        PQfinish(target_conn);

                        result["success"] = true;
                        result["allMatch"] = all_match;

                        loop->defer([response, result]() {
                            send_json_response(response, result);
                        });
                    });

                } catch (const std::exception& e) {
                    send_error_response(res, e.what(), 500);
                }
            },
            [res](const std::string& error) {
                send_error_response(res, error, 400);
            }
        );
    });

    // ========================================================================
    // POST /api/v1/migration/reset
    // Reset migration state back to idle
    // ========================================================================
    app->post("/api/v1/migration/reset", [ctx](auto* res, auto* req) {
        REQUIRE_AUTH(res, req, ctx, auth::AccessLevel::ADMIN);

        try {
            int current = g_migration_state.status.load();
            if (current == static_cast<int>(MigrationStatus::DUMPING) ||
                current == static_cast<int>(MigrationStatus::RESTORING)) {
                send_error_response(res, "Cannot reset while migration is in progress", 409);
                return;
            }

            g_migration_state.reset();
            nlohmann::json response = {{"status", "idle"}, {"message", "Migration state reset"}};
            send_json_response(res, response);
        } catch (const std::exception& e) {
            send_error_response(res, e.what(), 500);
        }
    });
}

} // namespace routes
} // namespace queen
