#include <db/db.h>
#include <cstdlib>
#include <log.h>

MAKE_STREAM_STRUCT(std::cerr, "db: ", db)

namespace hydra::db {

namespace helper {
struct disconnector {
  disconnector() = default;
  ~disconnector() {
    log::db << "destroying disconnector" << std::endl;
    disconnect();
  }
};
disconnector db_disconnector;
}

PGconn *conn = nullptr;

void connect() {
  if (conn)return;
  conn = PQconnectdb(HYDRA_LIBS_DB_CONNECTION_STRING);
  if (PQstatus(conn) != CONNECTION_OK) {
    log::fatal << "connection to database failed: " << PQerrorMessage(conn);
    PQfinish(conn);
    exit(1);
  }
  log::db << "connected to database " << single_result_query("select current_database();") << std::endl;
}

void disconnect() {
  if (conn == nullptr)return;
  PQfinish(conn);
  conn = nullptr;
  log::db << "disconnected from database" << std::endl;
}

namespace {
PGresult *execute_or_die(std::string_view query, ExecStatusType expected_status) {
  connect();
  PGresult *res = PQexec(conn, std::string(query).c_str());
  if (PQresultStatus(res) != expected_status) {
    log::fatal << PQresultStatus(res) << std::endl;
    log::fatal << "query failed: " << PQresultErrorMessage(res) << std::endl;
    exit(1);
  }
  return res;
}
}

void execute_command(std::string_view query) {
  PGresult *res = execute_or_die(query, PGRES_COMMAND_OK);
  PQclear(res);
}

std::string single_result_query(std::string_view query) {
  PGresult *res = execute_or_die(query, PGRES_TUPLES_OK);
  if (PQntuples(res) != 1 || PQnfields(res) != 1) {
    log::fatal << "returned unexpected number of results: " << query << std::endl;
    exit(1);
  }
  std::string result(PQgetvalue(res, 0, 0));
  PQclear(res);
  return result;
}

template<auto F>
inline auto inline_single_result_query_processed(std::string_view query) {
  PGresult *res = execute_or_die(query, PGRES_TUPLES_OK);
  if (PQntuples(res) != 1 || PQnfields(res) != 1) {
    log::fatal << "returned unexpected number of results: " << query << std::endl;
    exit(1);
  }
  auto result = F(PQgetvalue(res, 0, 0));
  PQclear(res);
  return result;
}

template<auto F, typename T>
inline auto inline_single_result_query_processed_orelse(std::string_view query, T alternative) {
  PGresult *res = execute_or_die(query, PGRES_TUPLES_OK);
  int nrows = PQntuples(res);
  if (nrows > 1 || PQnfields(res) != 1) {
    log::fatal << "returned unexpected number of results: " << query << std::endl;
    exit(1);
  }
  if (nrows == 0) {
    PQclear(res);
    return alternative;
  }
  auto result = F(PQgetvalue(res, 0, 0));
  PQclear(res);
  return result;
}

uint64_t single_uint64_query(std::string_view query) {
  return inline_single_result_query_processed<strtoull>(query);
}

uint64_t single_uint64_query_orelse(std::string_view query, const uint64_t fallback) {
  return inline_single_result_query_processed_orelse<strtoull>(query, fallback);
}

int count_rows(std::string_view query) {
  PGresult *res = execute_or_die(query, PGRES_TUPLES_OK);
  int rows = PQntuples(res);
  PQclear(res);
  return rows;
}
inline uint64_t strtoull(const char *s) {
  return std::strtoull(s, nullptr, 10);
}

execution::execution(const uint64_t execution_id) {
  PGresult *res = execute_or_die(std::string(
      "select executions.id execution_id,executions.state_id execution_state, job_id, jobs.state_id job_state,session_id,sessions.state_id session_state,  checkpoint_policy_id, command, environment_id  from executions left join jobs on jobs.id=executions.job_id left join sessions on sessions.id=executions.session_id where executions.id = ").append(
      std::to_string(execution_id)).append(";"), PGRES_TUPLES_OK);
  if (PQntuples(res) != 1) {
    log::fatal << " no execution matches id " << id << std::endl;
    exit(1);
  }
  id = strtoull(PQgetvalue(res,0,0));
  state = strtoull(PQgetvalue(res,0,1));
  job_id = strtoull(PQgetvalue(res,0,2));
  job_state = strtoull(PQgetvalue(res,0,3));
  session_id = strtoull(PQgetvalue(res,0,4));
  session_state = strtoull(PQgetvalue(res,0,5));
  chekpoint_policy = strtoull(PQgetvalue(res,0,6));
  command.assign(PQgetvalue(res,0,7));
  environment_id = strtoull(PQgetvalue(res,0,8));
  PQclear(res);
};

}