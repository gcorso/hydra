#ifndef HYDRA_LIBS_DB_DB_H_
#define HYDRA_LIBS_DB_DB_H_
#include <string>
#include <log/log.h>
#include <utility>
#include <tuple>
#include <vector>
#include <db/config.h>
#include <mutex>
#include <cassert>

namespace hydra {
class database_error : public std::runtime_error {
  using std::runtime_error::runtime_error;
};

class transaction;

class db_base {

 private:
  PGresult *execute_single_(std::string_view query, ExecStatusType expected_status);

 public:


  db_base(); //not pointing to anything
  db_base(const db_base &) = delete;
  db_base(db_base &&) = delete;
  ~db_base();
  void execute_command(std::string_view query);
  std::string single_result_query(std::string_view query);
  uint64_t single_uint64_query(std::string_view query);

 protected:
  db_base(PGconn *conn) : conn_(conn) {} // to be used for instantiating transactions
  transaction start_transaction();


  PGconn *conn_;
  std::mutex mutex_;

  template<auto F>
  inline auto inline_single_result_query_processed(std::string_view query) {
    PGresult *res = execute_single_(query, PGRES_TUPLES_OK);
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
    PGresult *res = execute_single_(query, PGRES_TUPLES_OK);
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
  /*
struct data_binder;
inline uint64_t strtoull(const char *s);
uint64_t raw_tou64(const char *c);
void connect();
void disconnect();
std::string single_result_query(std::string_view query);
std::string single_result_query_orelse(std::string_view query, std::string_view def = "");
void execute_command(std::string_view);
void execute_command(std::string_view, const data_binder &);
int count_rows(std::string_view query);
uint64_t single_uint64_query_orelse(std::string_view query, const uint64_t);
void keep_session_alive(const uint64_t);
void clean_sessions();

struct execution {
  uint64_t id, job_id, session_id, environment_id;
  int state, session_state, job_state, chekpoint_policy;
  std::string command;
  execution() = delete;
  execution(const uint64_t id);
};

struct checkpoint {
  uint64_t id;
  std::string value;
  static checkpoint retrieve(uint64_t job_id);
};

struct data_binder {
  std::vector<const char *> values; //begins
  std::vector<int> lengths; //sizes
  std::vector<int> binary; //binaries
  data_binder() {}
  data_binder(std::initializer_list<std::pair<const char *, int> > il) {
    for (auto[p, l] : il)push_back(p, l);
  }
  template<typename T>
  void push_back(const T *ptr, int length) {
    values.push_back((const char *) ptr);
    lengths.push_back(length);
    binary.push_back(1);
  }
  template<typename T>
  void push_back_nonbinary(const T *ptr, int length) {
    values.push_back((const char *) ptr);
    lengths.push_back(length);
    binary.push_back(0);
  }
  template<typename T>
  void push_back(const T *ptr, const T *ptr_end) {
    values.push_back((const char *) ptr);
    lengths.push_back(ptr_end - ptr);
    binary.push_back(1);
  }
};
*/
};

class transaction : public db_base {
  friend class db_base;

  transaction(PGconn *c, std::mutex& f) : db_base(c) , fmutex_(f) {
    execute_command("START TRANSACTION");
  }
 public:
  void abort() {
    assert(conn_!= nullptr);
    execute_command("ABORT TRANSACTION");
    conn_ = nullptr;
    fmutex_.unlock();
  }
  void commit() {
    assert(conn_!= nullptr);
    execute_command("COMMIT TRANSACTION");
    conn_ = nullptr;
    fmutex_.unlock();
  }
  ~transaction(){
    if(conn_){
      abort();
    }
  }
 private:
  std::mutex &fmutex_;
};

class db : public db_base {
 public:
  using db_base::start_transaction;
 private:
 public:
  static db awsdb;
};
}

#endif //HYDRA_LIBS_DB_DB_H_
