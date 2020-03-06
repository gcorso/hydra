#ifndef HYDRA_LIBS_DB_INCLUDE_DB_DB_H_
#define HYDRA_LIBS_DB_INCLUDE_DB_DB_H_
#include <string>
#include <log.h>
#include "config.h"

namespace hydra::db{

inline uint64_t strtoull(const char *s);

extern PGconn *conn;
void connect();
void disconnect();
std::string single_result_query(std::string_view query);
void execute_command(std::string_view);
int count_rows(std::string_view query);
uint64_t single_uint64_query(std::string_view query);
uint64_t single_uint64_query_orelse(std::string_view query,const uint64_t);

struct execution {
  uint64_t id,job_id,session_id,environment_id;
  int state, session_state,job_state,chekpoint_policy;
  std::string command;
  execution() = delete;
  execution(const uint64_t id);
};

}

#endif //HYDRA_LIBS_DB_INCLUDE_DB_DB_H_
