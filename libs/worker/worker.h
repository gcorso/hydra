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

using util::strjoin;
using namespace std::chrono_literals;

class session {
 public:
  struct params {
    std::chrono::milliseconds keepalive_interval = 60s;
  };
  const uint64_t id;
  session() : id(0) {}
  session(const params &p) : id(db::awsdb.single_uint64_query("INSERT INTO sessions DEFAULT VALUES RETURNING id")) {
    std::cerr << "new session with id "<<id<<std::endl;
    keepalive.run([session_id = id]()  {
      db::awsdb.execute_command(strjoin("update sessions set time_last = current_timestamp(6) where id = ",session_id));
      std::cerr << "keepalive" << std::endl;
    }, p.keepalive_interval);
  }
  ~session() {
    if (id == 0)return;
    keepalive.kill();
    db::awsdb.execute_command(strjoin("UPDATE sessions SET time_end = current_timestamp(6), state_id = 2 where id = ", id, " ;"));
  }
 private:
  repeater keepalive;
};

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
