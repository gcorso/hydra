#include <worker/worker.h>
#include <iostream>
#include <log.h>
#include <sys/stat.h>


namespace hydra::worker {
//declaration of all extern members
uint64_t id;
uint64_t session_id = 0;
std::string nick;
const std::string WORKER_DIR = std::string(getenv("HOME")).append("/.worker");

namespace {




void session_end();




uint64_t retrieve_file_id() {
  FILE *fp = fopen((WORKER_DIR + "/id").c_str(), "rb");
  if (!fp)return 0;
  uint64_t id;
  fread(&id, sizeof(id), 1, fp);
  fclose(fp);
  return id;
}

void store_file_id() {
  if (mkdir(WORKER_DIR.c_str(), 0777) && errno != EEXIST) {
    log::fatal << "could not create worker directory in  " << WORKER_DIR << "(errno: " << errno << ")" << std::endl;
    exit(1);
  }
  FILE *fp = fopen((WORKER_DIR + "/id").c_str(), "wb");
  if (!fp) {
    log::fatal << "could not store worker id in " << (WORKER_DIR + "/id") << "(errno: " << errno << ")" << std::endl;
    exit(1);
  }
  fwrite(&id, sizeof(id), 1, fp);
  fclose(fp);
}
void setup_worker_new() {
  log::worker << "new worker, creating id" << std::endl;

 /* PGresult *res = PQexec(dbconn, "INSERT INTO workers DEFAULT VALUES RETURNING id;");
  if (PQresultStatus(res) != PGRES_TUPLES_OK) {
    log::fatal << PQresultStatus(res) << std::endl;
    log::fatal << "insert into table failed: " << PQresultErrorMessage(res) << std::endl;
    exit(1);
  }
  id = std::strtoull(PQgetvalue(res, 0, 0), nullptr, 10);*/
  store_file_id();
}

void setup_worker_existing() {
  log::worker << "retrieved id from last session" << std::endl;
  /*PGresult *res = PQexec(dbconn, std::string("SELECT id FROM workers WHERE id = ").append(std::to_string(id)).append(";").c_str());
  if (PQresultStatus(res) != PGRES_TUPLES_OK) {
    log::fatal << PQresultStatus(res) << std::endl;
    log::fatal << "insert into table failed: " << PQresultErrorMessage(res) << std::endl;
    exit(1);
  }
  if (!PQntuples(res)) {
    log::worker << "invalid last session: worker reset" << std::endl;
    setup_worker_new();
  } else {
    log::worker << "found id in db" << std::endl;
  }*/



}


void session_start(){
/*  PGresult *res = PQexec(dbconn, std::string("INSERT INTO sessions (worker_id) VALUES (").append(std::to_string(id)).append(") RETURNING id;").c_str());
  if (PQresultStatus(res) != PGRES_TUPLES_OK) {
    log::fatal << PQresultStatus(res) << std::endl;
    log::fatal << "insert into table failed: " << PQresultErrorMessage(res) << std::endl;
    exit(1);
  }
  session_id = std::strtoull(PQgetvalue(res, 0, 0), nullptr, 10);
*/
}

void session_end(){
  /*
  if(session_id==0)return;
  PGresult *res = PQexec(dbconn, std::string("UPDATE  sessions  SET time_end = CURRENT_TIMESTAMP WHERE id = ").append(std::to_string(session_id)).append(";").c_str());
  if (PQresultStatus(res) != PGRES_COMMAND_OK) {
    log::fatal << PQresultStatus(res) << std::endl;
    log::fatal << "update into table failed: " << PQresultErrorMessage(res) << std::endl;
    exit(1);
  }
  session_id = 0;*/
}

void setup_worker() {
  id = retrieve_file_id();
  if (id) setup_worker_existing(); else setup_worker_new();
  log::worker << "id: " << id << std::endl;
  session_start();
}

}
void job_loop() {
  int idleness = 0;
  constexpr  int IDLENESS_LIMIT = 10;
  while(idleness<IDLENESS_LIMIT){
    uint64_t job_id = 0;/*
    PGresult *res = PQexec(dbconn, "SELECT id FROM jobs WHERE job_status_id = 2 LIMIT 1");
    EXPECT_RES(res,PGRES_TUPLES_OK);
    log::worker << PQntuples(res) << std::endl;
    if(PQntuples(res)){
      PQclear(res);
      res = PQexec(dbconn, "BEGIN ISOLATION LEVEL SERIALIZABLE;");
      EXPECT_RES(res,PGRES_COMMAND_OK);
      PQclear(res);
      res = PQexec(dbconn,"UPDATE jobs "
                          "SET    job_status_id = 3 "
                          "WHERE  id = (SELECT id FROM jobs WHERE job_status_id = 2 LIMIT 1) "
                          "RETURNING * ;");
      EXPECT_RES(res,PGRES_TUPLES_OK);
      log::worker << PQntuples(res) << std::endl;
      job_id = std::strtoull(PQgetvalue(res, 0, 0), nullptr, 10);
      PQclear(res);
      res = PQexec(dbconn, "COMMIT TRANSACTION;");
      EXPECT_RES(res,PGRES_COMMAND_OK);
      PQclear(res);
    }
    idleness=IDLENESS_LIMIT;*/
   log::worker << "job_id: "<<job_id << std::endl;
  }
}


void work() {
  setup_worker();
  job_loop();

}


}