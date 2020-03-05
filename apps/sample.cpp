
#include <db/db.h>
#include <iostream>
#include <log.h>

MAKE_STREAM_STRUCT(std::cerr,"sample: ",sample)
using namespace hydra;
int main(){
  db::connect();
  log::sample << "succesfully connected to database "<<std::endl;
  exit(1);

}