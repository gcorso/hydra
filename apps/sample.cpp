
#include <db/db.h>
#include <iostream>
#include <log.h>

MAKE_STREAM_STRUCT(std::cerr,"sample: ",sample)
using namespace hydra;
int main(){

  /*std::string dbname = db::single_result_query("select current_database();");
  log::sample << "succesfully connected to database " << dbname << " with user " << db::single_result_query("select current_user;")<< std::endl;
  uint64_t next_id = db::single_result_query_processed<db::strtoull>("select nextval('id_seq');");
  log::sample << "next_id: " << next_id <<std::endl;*/
}