#include <worker/worker.h>
#include <iostream>
#include <log.h>
#include <sys/stat.h>
#include <atomic>
#include <csignal>
#include <cassert>
#include <zconf.h>
#include <sstream>
#include <db/db.h>
#include <marshall/marshall.h>

namespace hydra::worker {

uint64_t id;
const std::string WORKER_DIR = std::string(getenv("HOME")).append("/.worker");

namespace status {
enum loc_t {
  L0, L1, L2, L3, L4, L5, L6, L7, L8, L9, L10, L11, L_DELAYED_SIGINT
};
std::atomic<loc_t> location(L0);
uint64_t session_id = 0, execution_id = 0;

}

namespace {

namespace util {
template<typename ...Args>
struct joiner;
template<>
struct joiner<> {
  static inline void append_to(std::stringstream &cref) { return; }
};
template<typename T, typename...Ts>
struct joiner<T, Ts...> {
  static inline void append_to(std::stringstream &cref, const T &t, Ts &&... ts) {
    cref << t;
    joiner<Ts...>::append_to(cref, std::forward<Ts>(ts)...);
  }
};

}

template<typename ...Ts>
std::string strjoin(Ts &&... ts) {
  std::stringstream ss;
  util::joiner<Ts...>::append_to(ss, std::forward<Ts>(ts)...);
  return ss.str();
}

}

namespace setup {

uint64_t retrieve_file_id() {
  FILE *fp = fopen((WORKER_DIR + "/id").c_str(), "rb");
  if (!fp)return 0;
  uint64_t id;
  fread(&id, sizeof(id), 1, fp);
  fclose(fp);
  return id;
}

void store_file_id() {
  if (mkdir(WORKER_DIR.c_str(), 0777) && errno != EEXIST) {
    log::fatal << "could not create worker directory in  " << WORKER_DIR << "(errno: " << errno << ")" << std::endl;
    exit(1);
  }
  FILE *fp = fopen((WORKER_DIR + "/id").c_str(), "wb");
  if (!fp) {
    log::fatal << "could not store worker id in " << (WORKER_DIR + "/id") << "(errno: " << errno << ")" << std::endl;
    exit(1);
  }
  fwrite(&id, sizeof(id), 1, fp);
  fclose(fp);
}
void worker_new() {
  log::worker << "new worker, creating id" << std::endl;
  id = db::single_uint64_query("INSERT into workers default values returning id;");
  store_file_id();
}

void worker_existing() {
  log::worker << "retrieved id from last session, " << std::endl;
  if (db::count_rows(strjoin("SELECT id FROM workers WHERE id = ", id, ";"))) {
    log::worker << "and it's valid" << std::endl;
  } else {
    log::worker << "but it's unvalid" << std::endl;
    worker_new();
  }
}

void setup() {
  id = retrieve_file_id();
  if (id) worker_existing(); else worker_new();
  log::worker << "id: " << id << std::endl;
}

}

namespace session {

void open() {
  status::session_id = db::single_uint64_query(strjoin("INSERT into sessions (worker_id) values (", id, ") returning id;"));
  log::worker << "opened session " << status::session_id << std::endl;
}

void close() {
  if (status::session_id == 0)return;
  db::execute_command(strjoin("UPDATE sessions SET time_end = current_timestamp(6), state_id = 2 where id = ", status::session_id, " ;"));
  log::worker << "closed session " << status::session_id << std::endl;
  status::session_id = 0;
}

}

namespace execution {
void try_start() {
  db::execute_command("BEGIN;");
  uint64_t job_id = db::single_uint64_query_orelse("SELECT id FROM jobs WHERE state_id = 2 ORDER BY id FOR UPDATE SKIP LOCKED  LIMIT 1;", 0);
  if (job_id == 0) return db::execute_command("COMMIT TRANSACTION;");
  db::execute_command(strjoin("UPDATE jobs set state_id=3 WHERE id = ", job_id, ";"));
  status::execution_id = db::single_uint64_query(strjoin("insert into executions (session_id , job_id ) values (", status::session_id, ",", job_id, ") returning id;"));
  db::execute_command("COMMIT TRANSACTION;");
  log::worker << "started execution " << status::execution_id << std::endl;
}

void close() {
  if (status::execution_id == 0)return;
  log::worker << "closing execution " << status::execution_id << std::endl;
  db::execute_command("BEGIN;");
  uint64_t job_id = db::single_uint64_query(strjoin("update executions set state_id = 5, time_end = current_timestamp(6) where id = ", status::execution_id, " returning job_id;"));
  db::execute_command(strjoin("update jobs set state_id = 4 where id = ", job_id, " ;"));
  db::execute_command("COMMIT;");
  status::execution_id = 0;
}

void drop() {
  if (status::execution_id == 0)return;
  log::worker << "dropping execution " << status::execution_id << std::endl;
  db::execute_command("BEGIN;");
  uint64_t job_id = db::single_uint64_query(strjoin("update executions set state_id = 3, time_end = current_timestamp(6) where id = ", status::execution_id, " returning job_id;"));
  db::execute_command(strjoin("update jobs set state_id = 2 where id = ", job_id, " ;"));
  db::execute_command("COMMIT;");
  status::execution_id = 0;
}

}

namespace sigint {

inline void sigint_teardown_exit() {
  //TODO: print that teardown might work with corrupted memory
  //TODO: teardown all the shit
  exit(0);
}

inline void sigint_disconnect_teardown_exit() {
  session::close();
  sigint_teardown_exit();
}

inline void sigint_drop_disconnect_teardown_exit() {
  execution::drop();
  sigint_disconnect_teardown_exit();
}

void handle_sigint(int signal_num) {
  log::worker << "received SIGINT at L" << status::location << ", terminating..." << std::endl;
  using namespace status;
  std::signal(SIGINT, SIG_IGN);
  switch (location.load()) {
    case L0: {
      sigint_teardown_exit();
    }
    case L1: {
      // connession might have been made; better wait L2
      location = L_DELAYED_SIGINT;
    }
    case L2: {
      sigint_disconnect_teardown_exit();
    };
    case L3: {
      //job might be broken; better wait call interrupt at L4
      location = L_DELAYED_SIGINT;
    };
    case L4: {
      if (status::execution_id)sigint_disconnect_teardown_exit();
      else sigint_drop_disconnect_teardown_exit();
    }
    case L7: {
      //99.999% of time location == L7.
      sigint_drop_disconnect_teardown_exit();
    }
    case L5:
    case L6: {
      sigint_disconnect_teardown_exit();
    }
    case L8: {
      //job is finished, and we might be already uploading; should call interrupt at L9
      location = L_DELAYED_SIGINT;
    }
    case L9:
    case L10:
    case L11: {
      sigint_disconnect_teardown_exit();
    }
    case L_DELAYED_SIGINT: {
      //this cannot possibly happen
      exit(-1);
    }
  }
}

}

void work() {

  status::location = status::L1;
  std::signal(SIGINT, sigint::handle_sigint);
  setup::setup();
  session::open();
  status::loc_t expl1 = status::L1;
  if (!status::location.compare_exchange_strong(expl1, status::L2)) {
    assert(expl1 == status::L_DELAYED_SIGINT);
    status::location = status::L2;
    sigint::handle_sigint(SIGINT);
  }

  static constexpr size_t IDLENESS_LIMIT_CYCLE = 20, WORK_POLLING_CYCLE_LENGTH = 30;

  size_t idle_cycles_left = IDLENESS_LIMIT_CYCLE;

  db::clean_sessions();
  while (idle_cycles_left) {
    status::location = status::L3;
    execution::try_start();
    status::loc_t expl3 = status::L3;
    if (!status::location.compare_exchange_strong(expl3, status::L4)) {
      assert(expl3 == status::L_DELAYED_SIGINT);
      status::location = status::L4;
      sigint::handle_sigint(SIGINT);
    }
    if (status::execution_id == 0) {
      log::worker << "no job assigned, will try again " << idle_cycles_left << " times." << std::endl;
      status::location = status::L5;
      --idle_cycles_left;
      sleep(WORK_POLLING_CYCLE_LENGTH);
      db::keep_session_alive(status::session_id);
      db::clean_sessions();
      status::location = status::L6;
    } else {
      status::location = status::L7;
      idle_cycles_left = IDLENESS_LIMIT_CYCLE;
      marshall::marshall(status::execution_id, status::session_id);
      status::location = status::L8;
      execution::close();
      status::loc_t expl8 = status::L8;
      if (!status::location.compare_exchange_strong(expl8, status::L9)) {
        assert(expl8 == status::L_DELAYED_SIGINT);
        status::location = status::L9;
        sigint::handle_sigint(SIGINT);
      }
    }
    status::location = status::L10;
  }

  status::location = status::L11;

  session::close();
  //TODO: teardown();
  exit(0);

}

}