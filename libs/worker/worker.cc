#include <worker/worker.h>
#include <fcntl.h>

#include <boost/program_options.hpp>
#include <fstream>
#include <iostream>
namespace po = boost::program_options;

namespace hydra {

std::string get_dbconn_string() {
  std::ifstream settings_file("config.ini");
  po::variables_map vm;
  std::string dbconnstring;
  po::options_description desc;
  desc.add_options()("dbconnstring", po::value<std::string>(&dbconnstring));
  po::store(po::parse_config_file(settings_file, desc), vm);
  po::notify(vm);
  std::cout << dbconnstring << std::endl;
  return dbconnstring;
}

namespace {
constexpr const char *const WORKER_ID_PATH = "~/.worker/id";
uint64_t retrieve_file_id() {
  FILE *fp = fopen(WORKER_ID_PATH, "rb");
  if (!fp)return 0;
  uint64_t id;
  fread(&id, sizeof(id), 1, fp);
  fclose(fp);
  return id;
}

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
    std::cerr << "connection to db failed: "<< e.what() << std::endl;
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
}
}