#ifndef HYDRA_LIBS_WORKER_INCLUDE_WORKER_WORKER_H_
#define HYDRA_LIBS_WORKER_INCLUDE_WORKER_WORKER_H_

#include <cstdint>
#include <string>
#include <pqxx/connection>

namespace hydra {

class worker {
 public:
  worker();
  ~worker();
  void run();
 private:
  void setup_new();
  void setup_existing();
  uint64_t id;
  std::string nick;
  pqxx::connection* dbconn;
};

}

#endif //HYDRA_LIBS_WORKER_INCLUDE_WORKER_WORKER_H_
