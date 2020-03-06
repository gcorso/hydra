
#include <db/db.h>
#include <iostream>
#include <log.h>
#include <cassert>
#include <zconf.h>

MAKE_STREAM_STRUCT(std::cerr,"sample: ",sample)
using namespace hydra;
int main(){
  db::connect();

  //we need to convert the number into network byte order
  std::string data = "\"S\\\xff";// three characters: quote, capital S, backslash
  assert(data.size()==4);
  db::execute_command("UPDATE executions SET stdout = stdout || $1::bytea WHERE id = 154",db::data_binder({{data.data(),data.size()}}));

  pid_t pid = fork();

  if(pid==0){
    if (execlp("bash", "bash","-c", "sleep 4;echo Hello;sleep 2;", nullptr) == -1) {
      std::perror("subprocess: execlp() failed");
      exit(-1);
    }
  }


  struct raii_char_str {
    raii_char_str(std::string s) : buf(s.c_str(), s.c_str() + s.size() + 1) {};
    operator char *() const { return &buf[0]; };
    mutable std::vector<char> buf;
  };

  std::string cmd = "bash";
  std::vector<std::string> argv = {"-c", "sleep 4;echo Hello;sleep 2;"};

  argv.insert(argv.begin(), cmd);
  std::vector<raii_char_str> real_args(argv.begin(), argv.end());
  std::vector<char *> cargs(real_args.begin(), real_args.end());
  cargs.push_back(nullptr);

  if (execvp(cargs[0], cargs.data()) == -1) {
    std::perror("subprocess: execvp() failed");
    exit(1);
  }

}