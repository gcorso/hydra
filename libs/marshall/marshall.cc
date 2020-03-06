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

MAKE_STREAM_STRUCT(std::cerr, "marshall: ", marshall)

namespace hydra::marshall {

int get_env_fd() {
  mkdir("/tmp/nullenv", 0777);
  int fd = open("/tmp/nullenv", O_RDWR);
  return fd;
}

namespace buffer {
static constexpr int BUFFER_LENGTH = 1000000;
static char data[BUFFER_LENGTH];
}

struct dynamic_buffer {
  int fd;
  dynamic_buffer(int fd_no) : fd(fd_no) {}
  std::string content;
  int read() {
    int read_bytes = ::read(fd, buffer::data, buffer::BUFFER_LENGTH);
    std::cerr << "fd"<<fd<<" read: "<<read_bytes<<" bytes! "<<std::endl;
    if (read_bytes > 0) {
      content.append(buffer::data, read_bytes);
      //std::cerr << "fd"<<fd<<" received: "<<std::string_view(buffer::data,read_bytes);
    }
    return read_bytes;
  }
  void close() {std::cerr << "fd"<<fd<<" closed."<<std::endl;}
  inline std::size_t size() { return content.size(); }
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
  void append_stream(const uint64_t execution_id,std::string_view streamname,char *data,int size) {
    db::execute_command(strjoin("UPDATE executions SET ",streamname," = ",streamname," || $1::bytea WHERE id = ",execution_id),db::data_binder({{data,size}}));
  }
}

struct stream_uploader : public dynamic_buffer {
  //using dynamic_buffer::dynamic_buffer;
  const uint64_t execution_id;
  const std::string_view streamname;
  stream_uploader(int fd,const uint64_t eid,std::string_view sn) : dynamic_buffer(fd),execution_id(eid),streamname(sn) {}
  int read() {
    int rb = dynamic_buffer::read();
    assert(rb>0);
    if(rb>0)upload::append_stream(execution_id,streamname,buffer::data,rb);
    std::cerr << streamname<<"["<<std::string_view(buffer::data,rb)<<"]"<<streamname<<std::endl;
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
    std::cerr << "retpoll: "<<retpoll<<std::endl;
    if (retpoll > 0)nactive = phelper<0, Tp, Ts...>::poll_loop(ptrs, active, fdtab, fdtab, nactive);
    std::cerr << "nactive: "<<nactive<<std::endl;
    return retpoll;
  }

};

void marshall(const uint64_t execution_id) {
  db::execution execution(execution_id);
  log::marshall << "executing command \"" << execution.command << "\"" << std::endl;

  //TODO: prepare environment and get fd of directory where the job shall be run
  //TODO: fork marshall/process

  int dir = get_env_fd();

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
    fchdir(dir);

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
    if (execlp("sh", "sh","-c", execution.command.c_str()) == -1) {
      std::perror("subprocess: execvp() failed");
      exit(-1);
    }

  }
  //parent process

  //close unused pipes
  ::close(out_pipe[WRITE]);
  ::close(err_pipe[WRITE]);

  stream_uploader su_out(out_pipe[READ],execution_id,"stdout"),su_err(err_pipe[READ],execution_id,"stderr");

  pollvector pv(&su_out, &su_err);
  while(pv.size_active()){
    if(pv.poll(2000)==0)log::marshall << "timeout" << std::endl;;
  }

  int status = 0;
  waitpid(pid, &status, 0);
  int exit_code = WEXITSTATUS(status);
  log::marshall << "exit_code: " << exit_code << std::endl;
  db::execute_command(strjoin("UPDATE executions SET exit_code = ",exit_code," WHERE id= ",execution_id));
}
}