
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
    if (execlp("bash", "bash","-c", "sleep 4;echo Hello;sleep 2;") == -1) {
      std::perror("subprocess: execvp() failed");
      exit(-1);
    }
  }

}