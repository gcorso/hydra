#ifndef HYDRA_LIBS_WORKER_WORKER_H_
#define HYDRA_LIBS_WORKER_WORKER_H_

#include <cstdint>
#include <string>
#include <db/db.h>
#include <atomic>
#include <functional>
#include <util/util.h>
#include <util/repeater.h>

namespace hydra {

class worker {
 public:
  struct params {
    int execution_directory = 0;
    int IN_FILENO = 0;
    int OUT_FILENO = 1;
    int ERR_FILENO = 2;
  };
  enum class state {
    NONE = 0, //default initialized, or detached, or joined. can be safely destroyed
    WAITING = 1, //waiting for a job to come
    IDLENESS_EXCEEDED = 2, //
    RUNNING_JOB = 3
  };
  ~worker();
  template<typename _Callable>
  worker(const params &par, _Callable &&f) : state_(state::NONE), session_({.keepalive_interval=10s}) {
    callback_ = f;
    idleness = 0;
    idleness_limit = 10;

  }
  void wait();

 private:
  void act() {

  }
  int idleness_limit, idleness;
  std::atomic<state> state_;
  const session session_;
  std::function<void()> callback_;
};

}

#endif //HYDRA_LIBS_WORKER_WORKER_H_
