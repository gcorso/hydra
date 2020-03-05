#ifndef HYDRA_LIBS_WORKER_INCLUDE_WORKER_WORKER_H_
#define HYDRA_LIBS_WORKER_INCLUDE_WORKER_WORKER_H_

#include <cstdint>
#include <string>
#include <postgres/libpq-fe.h>

namespace hydra {

namespace worker {

  extern uint64_t id;
  extern std::string nick;
  extern PGconn * dbconn;
void work();

};

}

#endif //HYDRA_LIBS_WORKER_INCLUDE_WORKER_WORKER_H_
