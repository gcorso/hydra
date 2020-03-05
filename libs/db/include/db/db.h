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

}

#endif //HYDRA_LIBS_DB_INCLUDE_DB_DB_H_
