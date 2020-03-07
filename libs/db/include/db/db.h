#ifndef HYDRA_LIBS_DB_INCLUDE_DB_DB_H_
#define HYDRA_LIBS_DB_INCLUDE_DB_DB_H_
#include <string>
#include <log.h>
#include <utility>
#include <tuple>
#include <vector>
#include "config.h"

namespace hydra::db{

struct data_binder;
inline uint64_t strtoull(const char *s);
uint64_t raw_tou64(const char *c);

extern PGconn *conn;
void connect();
void disconnect();
std::string single_result_query(std::string_view query);
std::string single_result_query_orelse(std::string_view query,std::string_view def = "");
void execute_command(std::string_view);
void execute_command(std::string_view,const data_binder&);
int count_rows(std::string_view query);
uint64_t single_uint64_query(std::string_view query);
uint64_t single_uint64_query_orelse(std::string_view query,const uint64_t);
void keep_session_alive(const uint64_t);

struct execution {
  uint64_t id,job_id,session_id,environment_id;
  int state, session_state,job_state,chekpoint_policy;
  std::string command;
  execution() = delete;
  execution(const uint64_t id);
};

struct checkpoint {
  uint64_t id;
  std::string value;
  static checkpoint retrieve(uint64_t job_id);
};


struct data_binder{
  std::vector<const char *>values; //begins
  std::vector<int> lengths; //sizes
  std::vector<int> binary; //binaries
  data_binder(){}
  data_binder(std::initializer_list<std::pair<const char *,int> > il){
    for(auto [p,l] : il)push_back(p,l);
  }
  template<typename T>
  void push_back(const T* ptr,int length){
    values.push_back((const char *)ptr);
    lengths.push_back(length);
    binary.push_back(1);
  }
  template<typename T>
  void push_back(const T* ptr,const T* ptr_end){
    values.push_back((const char *)ptr);
    lengths.push_back(ptr_end-ptr);
    binary.push_back(1);
  }
};

}

#endif //HYDRA_LIBS_DB_INCLUDE_DB_DB_H_
