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
    std::cerr << "new session with id " << id << std::endl;
    keepalive.run([session_id = id]() {
      db::awsdb.execute_command(strjoin("update sessions set time_last = current_timestamp(6) where id = ", session_id));
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
    int idleness_limit;
    std::chrono::milliseconds wait_delay;
  };
  enum class state {
    NONE = 0, //default initialized, or detached, or joined. can be safely destroyed
    RUNNING = 1, //worker is running
    WAITING = 2,
    IDLENESS_EXCEEDED = 3 //callback was run
  };
  template<typename _Callable>
  worker(const params &par, _Callable &&f) : params_(par), state_(state::NONE), session_({.keepalive_interval=10s}) {
    callback_ = f;
    idleness = 0;
  }
  void run() {
    mutex_.lock();
    assert(state_ == state::NONE);
    state_ = state::WAITING;
    std::promise<void> p;
    join_ = p.get_future();
    mutex_.unlock();
    act(std::move(p));
  }
  bool joinable() {
    return state_ != state::NONE;
  }
  void join() {
    join_.wait();
    state_ = state::NONE;
  }
  ~worker() {
    mutex_.lock();
    assert(state_ == state::NONE || state_ == state::IDLENESS_EXCEEDED);
  }
  void kill() {
    mutex_.lock();
    if (state_ == state::NONE) {
      //TODO: consider throwing or logging double-kill
    } else if (state_ == state::IDLENESS_EXCEEDED) {
      // do nothing, and simply collect
    } else if (state_ == state::WAITING) {
      // do nothing
    } else if (state_ == state::RUNNING) {
      //TODO: kill executor
    }
    state_ = state::NONE;
    mutex_.unlock();


  }

 private:
  void act(std::promise<void> endp) {
    std::cerr << "acting" << std::endl;
    mutex_.lock();
    if (state_ == state::NONE) {
      mutex_.unlock();
      endp.set_value_at_thread_exit();
      return;
    }
    //try to get work
    auto t = db::awsdb.start_transaction();
    uint64_t job_id = t.single_uint64_query("SELECT COALESCE((SELECT id FROM jobs WHERE state_id = 2 ORDER BY id FOR UPDATE SKIP LOCKED  LIMIT 1),0)");
    if (job_id == 0) {
      t.abort();
      ++idleness;
      if (idleness >= params_.idleness_limit) {
        std::cerr << "idleness limit exceeded" << std::endl;
        state_ = state::IDLENESS_EXCEEDED;
        mutex_.unlock();
        callback_();
        endp.set_value_at_thread_exit();
        return;
      }
      state_ = state::WAITING;
      mutex_.unlock();
      std::cerr << "started wait" << std::endl;
      std::thread([this, delay = params_.wait_delay, p = std::move(endp)]() mutable {
        std::this_thread::sleep_for(delay);
        act(std::move(p));
      }).detach();
      return;
    }
    exit(177);
    //execution
    state_ = state::NONE;
    mutex_.unlock();
    idleness = 0;

  }
  int idleness;
  const params params_;
  state state_;
  std::mutex mutex_;
  const session session_;
  std::function<void()> callback_;
  std::future<void> join_;
};

}

#endif //HYDRA_LIBS_WORKER_WORKER_H_
