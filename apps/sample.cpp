
#include <db/db.h>
#include <iostream>
#include <log.h>
#include <cassert>
#include <zconf.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>

MAKE_STREAM_STRUCT(std::cerr,"sample: ",sample)
using namespace hydra;
int main(){
  db::connect();
  log::sample << "connected" << std::endl;
  db::clean_sessions();
}