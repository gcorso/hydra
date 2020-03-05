#ifndef HYDRA_LIBS_DB_INCLUDE_DB_DB_H_
#define HYDRA_LIBS_DB_INCLUDE_DB_DB_H_
#include <string>
#include <log.h>
#include "config.h"

namespace hydra::db{

inline uint64_t strtoull(const char *s){
return std::strtoull(s, nullptr, 10);
}

extern PGconn *conn;
void connect();
void disconnect();
PGresult *execute_or_die(std::string_view query,ExecStatusType expected_status);
std::string single_result_query(std::string_view query);
template<auto F>
auto single_result_query_processed(std::string_view query) {
  PGresult *res = execute_or_die(query,PGRES_TUPLES_OK);
  if(PQntuples(res)!=1 || PQnfields(res)!=1){
    log::fatal << "returned unexpected number of results: " <<query << std::endl;
    exit(1);
  }
  auto result = F(PQgetvalue(res, 0, 0));
  PQclear(res);
  return result;
}

}

#endif //HYDRA_LIBS_DB_INCLUDE_DB_DB_H_
