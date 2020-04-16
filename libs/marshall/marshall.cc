#include <marshall/marshall.h>
#include <db/db.h>
#include <log.h>
#include <zconf.h>
#include <wait.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <poll.h>
#include <cstring>
#include <tuple>
#include <sstream>
#include <cassert>
#include <unordered_set>
#include <algorithm>
#include <json.hpp>
#include <fstream>
#include <chrono>
#include <sys/mman.h>

MAKE_STREAM_STRUCT(std::cerr, "marshall: ", marshall)

namespace hydra::marshall {
using nlohmann::json;

namespace status {
uint64_t execution_id, job_id;
}

namespace buffer {
static constexpr int BUFFER_LENGTH = 1000000;
static char data[BUFFER_LENGTH];
}

struct dynamic_buffer {
  int fd;
  dynamic_buffer(int fd_no) : fd(fd_no) {}
  //std::string content;
  int read() {
    int read_bytes = ::read(fd, buffer::data, buffer::BUFFER_LENGTH);
    //std::cerr << "fd" << fd << " read: " << read_bytes << " bytes! " << std::endl;
    if (read_bytes > 0) {
      //content.append(buffer::data, read_bytes);
      //std::cerr << "fd" << fd << " received: " << std::string_view(buffer::data, read_bytes);
    }
    return read_bytes;
  }
  void close() { /*std::cerr << "fd" << fd << " closed." << std::endl;*/ }
};

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

namespace upload {
void append_stream(const uint64_t execution_id, std::string_view streamname, char *data, int size) {
  db::execute_command(strjoin("UPDATE executions SET ", streamname, " = ", streamname, " || $1::bytea WHERE id = ", execution_id), db::data_binder({{data, size}}));
}
}

struct stream_uploader : public dynamic_buffer {
  const uint64_t execution_id;
  const std::string_view streamname;
  stream_uploader(int fd, const uint64_t eid, std::string_view sn) : dynamic_buffer(fd), execution_id(eid), streamname(sn) {}
  int read() {
    int rb = dynamic_buffer::read();
    assert(rb > 0);
    if (rb > 0)upload::append_stream(execution_id, streamname, buffer::data, rb);
    std::cerr << std::string_view(buffer::data, rb);
    return rb;
  }
};

struct stream_uploader_pro : public dynamic_buffer {
  const uint64_t execution_id;
  const std::string_view streamname;
  stream_uploader_pro(int fd, const uint64_t eid, std::string_view sn) : dynamic_buffer(fd), execution_id(eid), streamname(sn), tail_n(0), last_upd(0) {}
  uint32_t tail_n = 0;
  std::string tail;
  uint32_t last_upd = 0;

  // at any moment (outside of read), DB contains (some text + \n) * + a prefix of length n of tail.
  // tail shall not contains newlines
  // we want to update DB on two occasions: newline, timer
  int read() {

    int rb = dynamic_buffer::read();
    assert(rb > 0);
    std::string write_on;
    for (int i = 0; i < rb; ++i) {
      char c = buffer::data[i];
      if (c == '\n') {
        //save
        write_on.append(std::move(tail));
        write_on.push_back(c);
        tail.clear();
      } else if (c == '\r') {
        //erase
        tail.clear();
      } else {
        //push
        tail.push_back(c);
      }
    }
    bool should_upd = false;
    if (!write_on.empty()) {
      //remove last n
      if (tail_n)db::execute_command(strjoin("UPDATE executions SET ", streamname, " = substring(", streamname, ",0,length(", streamname, ")-", tail_n, ") WHERE id = ", execution_id));
      tail_n = 0;
      // attach write on
      db::execute_command(strjoin("UPDATE executions SET ", streamname, " = ", streamname, " || $1::bytea WHERE id = ", execution_id), db::data_binder({{write_on.data(), write_on.size()}}));
      should_upd = true;
    }
    if (time(NULL) - last_upd > 20)should_upd = true;
    if (should_upd) {
      /*
      last_upd=time(NULL);
      if(tail_n)db::execute_command(strjoin("UPDATE executions SET ", streamname, " = substring(", streamname, ",0,length(",streamname,")-",tail_n,") WHERE id = ", execution_id));
      tail_n = tail.size();
      if(tail_n)db::execute_command(strjoin("UPDATE executions SET ", streamname, " = ", streamname, " || $1::bytea WHERE id = ", execution_id), db::data_binder({{tail.data(), tail.size()}}));
   */
    }
    std::cerr << std::string_view(buffer::data, rb);
    return rb;
  }
};

void execute_process_request(json);

struct stream_executor : public dynamic_buffer {
  using dynamic_buffer::dynamic_buffer;
  std::string buffered;
  int read() {
    int rb = dynamic_buffer::read();
    assert(rb > 0);
    char *b = buffer::data, *e = std::find(b, buffer::data + rb, 0);
    buffered.append(b, e);
    while (e != buffer::data + rb) {
      //execute and empty buffer
      execute_process_request(json::parse(buffered));
      buffered.clear();
      b = e + 1;
      e = std::find(b, buffer::data + rb, 0);
      buffered.append(b, e);
    }
    return rb;
  }
};

namespace {

template<int idx, typename Tp, typename ...Ts>
struct phelper;
template<int idx, typename Tp>
struct phelper<idx, Tp> {
  static inline void initial_fill(const Tp &tup, pollfd *dst) {}
  static inline int poll_loop(const Tp &tup, bool *act, pollfd *src, pollfd *dst, int rem) { return 0; }
};
template<int idx, typename Tp, typename T, typename ...Ts>
struct phelper<idx, Tp, T, Ts...> {
  static inline void initial_fill(const Tp &tup, pollfd *dst) {
    dst->fd = std::get<idx>(tup)->fd;
    dst->events = POLLIN;
    dst->revents = 0;
    phelper<idx + 1, Tp, Ts...>::initial_fill(tup, dst + 1);
  }
  static inline int poll_loop_old(const Tp &tup, bool *act, pollfd *src, pollfd *dst) {
    if (*act == 0)return phelper<idx + 1, Tp, Ts...>::initial_fill(tup, act + 1, src, dst);
    if (src->revents == 0) {
      dst->fd = std::get<idx>(tup)->fd;
      dst->events = POLLIN;
      dst->revents = 0;
      return phelper<idx + 1, Tp, Ts...>::initial_fill(tup, act + 1, src + 1, dst + 1);
    }
    if (src->revents & POLLIN)std::get<idx>(tup)->read();
    if (src->revents & POLLHUP) {
      std::get<idx>(tup)->close();
      *act = false;
      return phelper<idx + 1, Tp, Ts...>::initial_fill(tup, act + 1, src + 1, dst) + 1;
    }
    dst->fd = std::get<idx>(tup)->fd;
    dst->events = POLLIN;
    dst->revents = 0;
    return phelper<idx + 1, Tp, Ts...>::initial_fill(tup, act + 1, src + 1, dst + 1) + 1;
  }
  static inline int poll_loop(const Tp &tup, bool *act, pollfd *src, pollfd *dst, int rem) {
    if (rem == 0)return 0;
    int ret = 0;
    if (act) {

      if (src->revents & POLLIN)std::get<idx>(tup)->read();

      if ((src->revents & POLLHUP) == 0) {
        ++ret;
        dst->fd = std::get<idx>(tup)->fd;
        dst->events = POLLIN;
        dst->revents = 0;
        ++dst;
      } else {
        std::get<idx>(tup)->close();
        *act = false;
      }

      ++src;
    }
    return phelper<idx + 1, Tp, Ts...>::poll_loop(tup, act + 1, src, dst, rem - 1) + ret;
  }
};

}

template<typename ...Ts>
struct pollvector {
  typedef std::tuple<Ts...> Tp;
  static constexpr size_t nfds = std::tuple_size_v<Tp>;
  pollfd fdtab[nfds];
  Tp ptrs;
  bool active[nfds];
  int nactive = nfds;

  explicit pollvector(Ts &&... ts) : ptrs(std::forward<Ts>(ts)...) {
    memset(fdtab, 0, sizeof(fdtab));
    memset(active, 1, sizeof(active));
    phelper<0, Tp, Ts...>::initial_fill(ptrs, fdtab);
  }
  auto size() { return nfds; }
  auto size_active() { return nactive; }
  int poll(int timeout) {
    int retpoll = ::poll(fdtab, nactive, timeout);
    if (retpoll > 0)nactive = phelper<0, Tp, Ts...>::poll_loop(ptrs, active, fdtab, fdtab, nactive);
    return retpoll;
  }

};

namespace environment {

std::unordered_set<int> env_dirs;

void make_env(int env_id) {
  //special case


  if (env_id == 0) {
    rmdir("/tmp/__hydraenv_0__");
    if (mkdir("/tmp/__hydraenv_0__", 0777)) {
      log::fatal << "could not create directory: /tmp/__hydraenv_0__; errno: " << errno << std::endl;
      exit(1);
    }
    return;
  }

  //already in set
  if (env_dirs.find(env_id) != env_dirs.end()) {
    system(strjoin(" git -C /tmp/__hydraenv_", env_id, "__ pull -p; git -C /tmp/__hydraenv_", env_id, "__ clean -f -d;").c_str());
    return;
  }

  if (mkdir(strjoin("/tmp/__hydraenv_", env_id, "__").c_str(), 0777) && errno != EEXIST) {
    log::fatal << "could not create directory: /tmp/__hydraenv_" << env_id << "__; errno: " << errno << std::endl;
    exit(1);
  }

  std::string setup = db::single_result_query(strjoin("SELECT bash_setup FROM environments WHERE id = ", env_id));
  if (!setup.empty())system(setup.c_str());

  std::string gitlink = db::single_result_query(strjoin("SELECT gitlink FROM environments WHERE id = ", env_id));
  system(strjoin("git clone ", gitlink, " /tmp/__hydraenv_", env_id, "__; git -C /tmp/__hydraenv_", env_id, "__ pull -p; git -C /tmp/__hydraenv_", env_id, "__ clean -f -d;").c_str());
  env_dirs.insert(env_id);
}

}

static constexpr std::string_view REQUEST_TYPE_KEY = "_request_type", LINKED_FILES_KEY = "_linked_files", ETA_MILLIS_KEY = "millisec";

void execute_process_request(json req) {
  log::marshall << "got request" << req << std::endl;
  if (req.find(REQUEST_TYPE_KEY) == req.end()) {
    log::marshall << "got unvalid request" << std::endl;
    return;
  }
  std::string_view rt = req.find(REQUEST_TYPE_KEY)->get<std::string_view>();
  //log::marshall << "request type: " << rt << std::endl;
  if (rt != "save_checkpoint" && rt != "set_eta") {
    log::marshall << "unknown request type: " << rt << std::endl;
    return;
  }
  if (rt == "set_eta") {
    db::execute_command(strjoin("UPDATE executions SET time_eta = current_timestamp(6) + INTERVAL '",req.find(ETA_MILLIS_KEY)->get<uint64_t>()," milliseconds' WHERE id = ",status::execution_id));
    return;
  }

  req.erase(req.find(REQUEST_TYPE_KEY));
  auto linked_files = req.find(LINKED_FILES_KEY)->get<std::vector<std::string>>();
  req.erase(req.find(LINKED_FILES_KEY));
  db::execute_command("BEGIN;");
  db::execute_command(strjoin("delete from checkpoints where id in (select checkpoints.id from checkpoints left join executions on executions.id = checkpoints.execution_id where executions.job_id = ", status::job_id, ");"));
  uint64_t checkpoint_id = db::single_uint64_query(strjoin("INSERT INTO checkpoints (execution_id,value) VALUES (", status::execution_id, ",'", req.dump(), "') returning id;"));
  for (const std::string &path : linked_files) {
    log::marshall << "adding file: " << path << std::endl;
    struct stat s;
    int fd = open(path.c_str(), O_RDONLY);
    int status = fstat(fd, &s);
    const char *f = (const char *) mmap(nullptr, s.st_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
    db::execute_command(strjoin("INSERT INTO checkpoint_files (name,checkpoint_id,data) VALUES ('", basename(path.c_str()), "',", checkpoint_id, ", $1::bytea );"), db::data_binder({{f, s.st_size}}));
    close(fd);
  }
  db::execute_command("COMMIT;");

}

void marshall(const uint64_t execution_id, const uint64_t session_id) {
  status::execution_id = execution_id;
  db::execution execution(execution_id);
  status::job_id = execution.job_id;

  log::marshall << "running job " << execution.job_id << std::endl;
  log::marshall << "executing command \"" << execution.command << "\"" << std::endl;

  environment::make_env(execution.environment_id);
  puts("\033[0m");
  mkdir("/tmp/__hydra_resources__", 0777);
  system("rm -f /tmp/__hydra_checkpoint.json");
  system("rm -f /tmp/__hydra_control_pipe_out");
  mkfifo("/tmp/__hydra_control_pipe_out", 0666);

  if (execution.chekpoint_policy) {
    //put a checkpoint in place
    db::checkpoint ck = db::checkpoint::retrieve(execution.job_id);
    if (ck.id) {
      //there's a checkpoint
      json ckj = json::parse(ck.value);
      std::ofstream content_file("/tmp/__hydra_checkpoint.json");
      std::vector<std::string> retrieved_files;

      //raw query
      PGresult *res = PQexecParams(db::conn, strjoin("select id,name,data from checkpoint_files where checkpoint_id=", ck.id).c_str(), 0, NULL, NULL, NULL, NULL, 1);
      if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        log::fatal << PQresultStatus(res) << std::endl;
        log::fatal << "query failed: " << PQresultErrorMessage(res) << std::endl;
        exit(1);
      }
      for (int i = 0; i < PQntuples(res); ++i) {
        uint64_t id = db::raw_tou64(PQgetvalue(res, i, 0));
        mkdir(strjoin("/tmp/__hydra_resources__/", id).c_str(), 0777);
        std::string_view name(PQgetvalue(res, i, 1), PQgetlength(res, i, 1));
        int fd = open(strjoin("/tmp/__hydra_resources__/", id, "/", name).c_str(), O_WRONLY | O_CREAT, 0666);
        write(fd, PQgetvalue(res, i, 2), PQgetlength(res, i, 2));
        close(fd);
        retrieved_files.push_back(strjoin("/tmp/__hydra_resources__/", id, "/", name));
      }
      PQclear(res);

      //retrieve the files as well
      ckj[std::string(LINKED_FILES_KEY)] = retrieved_files;
      content_file << ckj.dump();
      content_file.flush();
      content_file.close();
    }
    //json ck = json::parse(db::single_result_query_orelse(strjoin("")));
  }

  static constexpr int READ = 0, WRITE = 1;
  int out_pipe[2];
  int err_pipe[2];


  //initialize pipes
  if (pipe2(out_pipe, O_NONBLOCK) == -1 || pipe2(err_pipe, O_NONBLOCK) == -1)
    throw std::system_error(errno, std::system_category());

  pid_t pid = ::fork();
  if (pid == 0) {
    //child process

    //change dir
    chdir(strjoin("/tmp/__hydraenv_", execution.environment_id, "__").c_str());

    //remap used pipes
    if (dup2(out_pipe[WRITE], STDOUT_FILENO) == -1 ||
        dup2(err_pipe[WRITE], STDERR_FILENO) == -1) {
      std::perror("subprocess: dup2() failed");
      exit(-1);
    }

    //close unused pipes
    if (out_pipe[READ] != -1) ::close(out_pipe[READ]);
    ::close(out_pipe[WRITE]);
    ::close(err_pipe[READ]);
    ::close(err_pipe[WRITE]);

    //run
    if (execlp("bash", "bash", "-c", execution.command.c_str(), nullptr) == -1) {
      std::perror("subprocess: execlp() failed");
      log::fatal << "errno: " << errno << std::endl;
      exit(-1);
    }

  }

  //close unused pipes
  ::close(out_pipe[WRITE]);
  ::close(err_pipe[WRITE]);

  stream_uploader su_out(out_pipe[READ], execution_id, "stdout"), su_err(err_pipe[READ], execution_id, "stderr");
  stream_executor control_stream(open("/tmp/__hydra_control_pipe_out", O_RDONLY | O_NONBLOCK));
  pollvector pv(&su_out, &su_err, &control_stream);
  db::keep_session_alive(session_id);
  while (pv.size_active() > 1) {
    pv.poll(60000);
    //log::marshall << "alive" << std::endl;
    db::keep_session_alive(session_id);
  }

  int status = 0;
  waitpid(pid, &status, 0);
  int exit_code = WEXITSTATUS(status);
  log::marshall << "exit_code: " << exit_code << std::endl;
  db::execute_command(strjoin("UPDATE executions SET exit_code = ", exit_code, " WHERE id= ", execution_id));
}
}