#ifndef HYDRA_LIBS_WORKER_WORKER_H_
#define HYDRA_LIBS_WORKER_WORKER_H_

#include <cstdint>
#include <string>
#include <db/db.h>
#include <atomic>
#include <functional>
#include <util/util.h>
#include <util/repeater.h>
#include <env_manager/env_manager.h>
#include <variant>

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
  void close() {
    if (id == 0)return;
    keepalive.kill();
    db::awsdb.execute_command(strjoin("UPDATE sessions SET time_end = current_timestamp(6), state_id = 2 where id = ", id, " ;"));

  }
  ~session() {
    close();
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
      executor_.kill();
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
    uint64_t job_id = t.single_uint64_query("SELECT COALESCE((SELECT id FROM jobs WHERE state_id = 2 AND batch_id = 1 ORDER BY id FOR UPDATE SKIP LOCKED  LIMIT 1),0)");
    if (job_id == 0) {
      t.abort();
      ++idleness;
      if (idleness >= params_.idleness_limit) {
        std::cerr << "idleness limit exceeded" << std::endl;
        state_ = state::IDLENESS_EXCEEDED;
        session_.close();
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
    //execution
    t.execute_command(strjoin("UPDATE jobs set state_id=3 WHERE id = ", job_id, ";"));
    uint64_t execution_id = t.single_uint64_query(strjoin("insert into executions (session_id , job_id ) values (", session_.id, ",", job_id, ") returning id;"));
    t.commit();
    idleness = 0;
    //prepare env
    std::promise<void> p = std::move(endp);
    std::async([job_id, execution_id, this](std::promise<void> p) {
      int env_fd = env_manager::make_environment(db::awsdb.single_uint64_query(strjoin("SELECT environment_id FROM jobs WHERE id = ", job_id, ";")));
      mutex_.lock();
      if (state_ == state::NONE) {
        mutex_.unlock();
        p.set_value_at_thread_exit();
        return;
      }
      state_ = state::RUNNING;
      std::cerr << "starting job" << std::endl;
      executor_.run(db::awsdb.single_result_query(strjoin("SELECT command FROM jobs WHERE id = ", job_id, ";")).c_str(), {.execution_directory = env_fd},
                    [job_id, execution_id, this](std::promise<void> p) {
                      std::cerr << "ended job" << std::endl;

                      mutex_.lock();
                      if (state_ == state::NONE) {
                        mutex_.unlock();
                        p.set_value_at_thread_exit();
                        return;
                      }

                      state_ = state::WAITING;
                      int ec = std::get<util::executor::returned>(executor_.join()).exit_code;
                      auto t = db::awsdb.start_transaction();
                      t.execute_command(strjoin("UPDATE jobs set state_id=4 WHERE id = ", job_id, ";"));
                      t.execute_command(strjoin("UPDATE executions set set state_id = 5, time_end = current_timestamp(6), exit_code=", ec, " where id = ", execution_id));
                      t.commit();

                      mutex_.unlock();
                      act(std::move(p));
                    }, std::move(p));
      mutex_.unlock();
    }, std::move(endp));
    mutex_.unlock();
  }
  int idleness;
  const params params_;
  state state_;
  std::mutex mutex_;
  session session_;
  std::function<void()> callback_;
  std::future<void> join_;
  util::executor executor_;
};

}

#endif //HYDRA_LIBS_WORKER_WORKER_H_
