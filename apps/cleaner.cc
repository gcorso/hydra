#include <db/db.h>
#include <iostream>
#include <algorithm>

int main(){
  hydra::db::clean_sessions();
  while(true) {
    uint64_t id = hydra::db::single_uint64_query_orelse("select id from checkpoints limit 1 ", 0);
    if(!id)break;
    std::cout << "removing ck "<<id<<std::endl;
    hydra::db::execute_command(std::string("DELETE from checkpoints where id = ").append(std::to_string(id)));
  }
  std::cout << "finished"<<std::endl;
  return 0;
}
