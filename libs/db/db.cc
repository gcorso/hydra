#include <db/db.h>
#include <cstdlib>
#include <log.h>
#include <sstream>

MAKE_STREAM_STRUCT(std::cerr, "db: ", db)


namespace hydra::db {

namespace {

namespace util {
template<typename ...Args>
struct joiner;
template<>
struct joiner<> {
  static inline void append_to(std::stringstream &cref) { return; }
};
template<typename T, typename...Ts>
struct joiner<T, Ts...> {
  static inline void append_to(std::stringstream &cref, const T &t, Ts &&... ts) {
    cref << t;
    joiner<Ts...>::append_to(cref, std::forward<Ts>(ts)...);
  }
};

}

template<typename ...Ts>
std::string strjoin(Ts &&... ts) {
  std::stringstream ss;
  util::joiner<Ts...>::append_to(ss, std::forward<Ts>(ts)...);
  return ss.str();
}

}

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

PGresult *execute_or_die_params(std::string_view query, ExecStatusType expected_status,const data_binder& bd) {
  connect();
  PGresult *res = PQexecParams(conn, std::string(query).c_str(),bd.binary.size(), nullptr,bd.values.data(),bd.lengths.data(),bd.binary.data(),0);
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

void execute_command(std::string_view query,const data_binder& db) {
  PGresult *res = execute_or_die_params(query, PGRES_COMMAND_OK,db);
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

std::string single_result_query_orelse(std::string_view query,std::string_view def){
  PGresult *res = execute_or_die(query, PGRES_TUPLES_OK);
  if (PQntuples(res) > 1 || PQnfields(res) != 1) {
    log::fatal << "returned unexpected number of results: " << query << std::endl;
    exit(1);
  }
  if(PQntuples(res)==0){
    std::string result(def);
    PQclear(res);
    return result;
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

void keep_session_alive(const uint64_t session_id){
  db::execute_command(strjoin("update sessions set time_last = current_timestamp(6) where id = ",session_id));
}

void clean_sessions(){
  db::execute_command("BEGIN ISOLATION LEVEL SERIALIZABLE;");
  PGresult *res = execute_or_die("update sessions set state_id = 3 , time_end = current_timestamp(6) where state_id = 1 and time_last < current_timestamp(6) - interval '10  minutes' returning id ;",PGRES_TUPLES_OK);
  size_t N = PQntuples(res);
  log::db << "cleaning "<< N << "  died sessions "<<std::endl;
  for(int i=0;i<N;i++){
    const uint64_t session_id = strtoull(PQgetvalue(res,i,0));
    log::db << "cleaning session_id:  "<< session_id << std::endl;
    PGresult *jobres = execute_or_die(strjoin("update executions set time_end = current_timestamp(6), state_id = 4 where session_id = ",session_id," and state_id = 2  returning job_id ;"),PGRES_TUPLES_OK);
    for(int j=0;j<PQntuples(jobres);++j){
      const uint64_t job_id = strtoull(PQgetvalue(jobres,j,0));
      log::db << "cleaning job_id:  "<< job_id << std::endl;
      execute_command(strjoin("update jobs set state_id = 2 where state_id = 3 and id = ",job_id));
    }
    PQclear(jobres);
  }
  PQclear(res);
  db::execute_command("COMMIT TRANSACTION;");
  log::db << "cleaning procedure of "<< N << "  died sessions went well"<<std::endl;
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

uint64_t raw_tou64(const char *c) {
  const uint8_t* array = (uint8_t*)c;
  return static_cast<uint64_t>(array[7]) |
          static_cast<uint64_t>(array[6]) << 8 |
          static_cast<uint64_t>(array[5]) << 16 |
          static_cast<uint64_t>(array[4]) << 24 |
          static_cast<uint64_t>(array[3]) << 32 |
          static_cast<uint64_t>(array[2]) << 40 |
          static_cast<uint64_t>(array[1]) << 48 |
          static_cast<uint64_t>(array[0]) << 56;
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

checkpoint checkpoint::retrieve(uint64_t job_id) {
  PGresult *res = execute_or_die(strjoin("select checkpoints.id checkpoint_id,value from checkpoints left join executions on executions.id=checkpoints.execution_id left join jobs on jobs.id=executions.job_id where job_id = ",job_id," ORDER BY time DESC,checkpoint_id DESC LIMIT 1"),PGRES_TUPLES_OK);
  if (PQntuples(res) == 0) {
    PQclear(res);
    return checkpoint{.id=0,.value=""};
  }
  checkpoint ck{.id=strtoull(PQgetvalue(res,0,0)),.value=PQgetvalue(res,0,1)};
  PQclear(res);
  return ck;
}

}