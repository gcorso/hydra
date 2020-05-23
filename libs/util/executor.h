
#ifndef HYDRA_LIBS_UTIL_EXECUTOR_H_
#define HYDRA_LIBS_UTIL_EXECUTOR_H_

namespace hydra{

class executor {
 public:
  struct params {
    int execution_directory = 0;
    int IN_FILENO = 0;
    int OUT_FILENO = 1;
    int ERR_FILENO = 2;
  };
 private:
  static pid_t fork_exec(const char *cmd, const params &p) {
    pid_t pid = fork();
    if (pid == 0) {
      //TODO: use params
      if (execlp("bash", "bash", "-c", cmd, nullptr) == -1) {
        std::perror("subprocess: execlp() failed");
        std::cerr << "errno: " << errno << std::endl;
        exit(-1);
      }
    }
    return pid;
  }
 public:
  struct returned { int exit_code; };
  struct signaled { int signal; };
  struct core_dumped {};
  typedef std::variant<returned, signaled, core_dumped> termination;
  enum class state {
    NONE = 0, //default initialized, or detached, or joined. can be safely destroyed
    RUNNING = 1, //
    EXITED = 2 // callback was run
    //thread only moves 1 to 2
  };
  executor() : pid_(0), state_(state::NONE) {};
  executor(const executor &) = delete;
  executor(executor &&) = delete;
  ~executor() {
    if (state_ != state::NONE)std::terminate;
  }
  termination join() {
    execution_.wait();
    pid_ = 0;
    state_ = state::NONE;
    int raw_state = execution_.get();
    //TODO: WCOREDUMP and WIFEXITED do not mutually exclude
    if (WCOREDUMP(raw_state))return core_dumped{};
    if (WIFEXITED(raw_state))return returned{WEXITSTATUS(raw_state)};
    if (WIFSIGNALED(raw_state))return signaled{WTERMSIG(raw_state)};
    //TODO: exception
  }
  std::pair<bool, termination> kill(std::chrono::microseconds patience = 500ms) {
    if (state previous = state::RUNNING;!state_.compare_exchange_strong(previous, state::NONE)) {
      assert(previous == state::EXITED);
      return {false, join()};
    }
    if (patience.count() && ::kill(pid_, SIGTERM) == -1) {
      if (errno != 3)exit(-1);
    }
    if (patience.count()==0 || execution_.wait_for(patience) == std::future_status::timeout) {
      if (::kill(pid_, SIGKILL) == -1) {
        if (errno != 3)exit(-1);
      }
    }
    return {true, join()};

  }
  template<typename Stringible, typename _Callable, typename... _Args>
  void run(const Stringible &command, const params &par, _Callable &&f, _Args &&... args) {
    pid_ = fork_exec(command, par);
    state_ = state::RUNNING;
    execution_ = std::async([&e = *this, pid = pid_, f, args...]() mutable -> int {
      int exit_status;
      int w = waitpid(pid, &exit_status, 0);
      assert(w == pid);
      if (state previous = state::RUNNING;!e.state_.compare_exchange_strong(previous, state::EXITED)) {
        assert(previous == state::NONE);
        return exit_status;
      }
      std::invoke(std::forward<_Callable>(f), std::forward<_Args>(args)...);
      return exit_status;
    });
  }
  bool joinable() const noexcept {return pid_;}
  pid_t pid() const noexcept {return pid_ ?: -1;}
 private:
  pid_t pid_;
  std::atomic<state> state_;
  std::future<int> execution_;
};

std::ostream &operator<<(std::ostream &o, const executor::returned &x) {
  o << "Process finished with exit code " << x.exit_code;
  return o;
}
std::ostream &operator<<(std::ostream &o, const executor::signaled &x) {
  o << "Process terminated with signal " << x.signal;
  return o;
}
std::ostream &operator<<(std::ostream &o, const executor::core_dumped &x) {
  o << "Segmentation fault (core dumped)";
  return o;
}
std::ostream &operator<<(std::ostream &o, const executor::termination &x) {
  std::visit([&o](const auto& v) { o << v; }, x);
  return o;
}
}

#endif //HYDRA_LIBS_UTIL_EXECUTOR_H_
