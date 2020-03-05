#include <worker/worker.h>
#include <fcntl.h>

#include <boost/program_options.hpp>
#include <fstream>
#include <iostream>
#include <log.h>
#include <sys/stat.h>
namespace po = boost::program_options;

namespace hydra::worker {
//declaration of all extern members
uint64_t id;
std::string nick;
PGconn *dbconn;
const std::string WORKER_DIR = std::string(getenv("HOME")).append("/.worker");

namespace {

void initialize_db_connection() {
  std::ifstream settings_file("config.ini");
  po::variables_map vm;
  std::string dbconnstring;
  po::options_description desc;
  desc.add_options()("dbconnstring", po::value<std::string>(&dbconnstring));
  po::store(po::parse_config_file(settings_file, desc), vm);
  po::notify(vm);
  if (dbconnstring.empty()) {
    log::fatal << "failed to retrieve config.ini, aborting" << std::endl;
    exit(1);
  }
  dbconn = PQconnectdb(dbconnstring.c_str());
  if (PQstatus(dbconn) != CONNECTION_OK) {
    log::fatal << "connection to database failed: " << std::string_view(PQerrorMessage(dbconn)) << std::endl;
    PQfinish(dbconn);
    exit(1);
  }
}

void gracefully_shutdown() {
  log::worker << "gracefully shutting down" << std::endl;
  PQfinish(dbconn);
  exit(0);
}

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

  PGresult *res = PQexec(dbconn, "INSERT INTO workers DEFAULT VALUES RETURNING id;");
  if (PQresultStatus(res) != PGRES_TUPLES_OK) {
    log::fatal << PQresultStatus(res) << std::endl;
    log::fatal << "insert into table failed: " << PQresultErrorMessage(res) << std::endl;
    exit(1);
  }
  id = std::strtoull(PQgetvalue(res, 0, 0), nullptr, 10);
  store_file_id();
}

void setup_worker_existing() {
  log::worker << "retrieved id from last session" << std::endl;
  PGresult *res = PQexec(dbconn, std::string("SELECT id FROM workers WHERE id = ").append(std::to_string(id)).append(";").c_str());
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
  }
}

void example_query() {
  PGresult *res = PQexec(dbconn, "INSERT INTO workers DEFAULT VALUES RETURNING id;");
  if (PQresultStatus(res) != PGRES_TUPLES_OK) {
    log::fatal << PQresultStatus(res) << std::endl;
    log::fatal << "insert into table failed: " << PQresultErrorMessage(res) << std::endl;
    exit(1);
  }
  std::cout << "Get " << PQntuples(res) << "tuples, each tuple has "
            << PQnfields(res) << "fields" << std::endl;
  // print column name
  for (int i = 0; i < PQnfields(res); i++) {
    std::cout << PQfname(res, i) << "              ";
  }
  std::cout << std::endl;
  // print column values
  for (int i = 0; i < PQntuples(res); i++) {
    for (int j = 0; j < PQnfields(res); j++) {
      std::cout << PQgetvalue(res, i, j) << "   ";
    }
    std::cout << std::endl;
  }
  PQclear(res);
}

void setup_worker() {
  id = retrieve_file_id();
  if (id) setup_worker_existing(); else setup_worker_new();
  log::worker << "id: " << id << std::endl;
}

}
/*
namespace {


}

worker::worker() {
  try {
    dbconn = new pqxx::connection(get_dbconn_string().append(" connect_timeout = 5"));
    if (dbconn->is_open()) {
      std::cout << "opened database successfully: " << dbconn->dbname() << std::endl;
    } else {
      std::cout << "could not open database" << std::endl;
      exit(1);
    }
  } catch (const std::exception &e) {
    std::cerr << "connection to db failed: " << e.what() << std::endl;
    exit(1);
  }

  id = retrieve_file_id();
  if (id) setup_existing(); else setup_new();
}

void worker::run() {

}
void worker::setup_new() {
  //dbconn->prepare("INSERT INTO workers DEFAULT VALUES");
}
void worker::setup_existing() {

}
worker::~worker() {
  dbconn->disconnect();
  delete dbconn;
}*/

void work() {
  initialize_db_connection();
  setup_worker();
  gracefully_shutdown();
}

}