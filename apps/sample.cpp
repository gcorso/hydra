
#include <db/db.h>
#include <iostream>
#include <log.h>
#include <cassert>

MAKE_STREAM_STRUCT(std::cerr,"sample: ",sample)
using namespace hydra;
int main(){
  db::connect();

  //we need to convert the number into network byte order
  std::string data = "\"S\\\xff";// three characters: quote, capital S, backslash
  assert(data.size()==4);
  db::execute_command("UPDATE executions SET stdout = stdout || $1::bytea WHERE id = 154",db::data_binder({{data.data(),data.size()}}));
  return 0;
//set the values to use
  const char *values[1] = {data.data()};
//calculate the lengths of each of the values
  int lengths[1] = {(int)data.size()};
//state which parameters are binary
  int binary[1] = {1};

  PGresult *res = PQexecParams(db::conn, "UPDATE executions SET stdout = $1::bytea WHERE id = 154",
                               1, //number of parameters
                               NULL, //ignore the Oid field
                               values, //values to substitute $1
                               lengths, //the lengths, in bytes, of each of the parameter values
                               binary, //whether the values are binary or not
                               0);
  if(PQresultStatus(res)!=PGRES_COMMAND_OK){
    log::fatal << PQresultStatus(res) << std::endl;
    log::fatal << "query failed: " << PQresultErrorMessage(res) << std::endl;
    exit(1);
  }
  /*std::string dbname = db::single_result_query("select current_database();");
  log::sample << "succesfully connected to database " << dbname << " with user " << db::single_result_query("select current_user;")<< std::endl;
  uint64_t next_id = db::single_result_query_processed<db::strtoull>("select nextval('id_seq');");
  log::sample << "next_id: " << next_id <<std::endl;*/
}