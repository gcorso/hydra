#include <iostream>
#include <string>
//#include <pqxx/pqxx>
#include <fstream>
#include <boost/program_options.hpp>
namespace po = boost::program_options;
#include <worker/worker.h>

int main() {
  //hydra::worker w;

  std::ifstream settings_file("config.ini");
  po::variables_map vm;
  std::string dbconnstring;
  po::options_description desc;
  desc.add_options()("dbconnstring", po::value<std::string>(&dbconnstring));
  po::store(po::parse_config_file(settings_file , desc), vm);
  po::notify(vm);
  pqxx::connection *dbconn;
  try {
    dbconn = new pqxx::connection(dbconnstring);
    if (dbconn->is_open()) {
      std::cout << "Opened database successfully: " << dbconn->dbname() << std::endl;
    } else {
      std::cout << "Can't open database" << std::endl;
      return 1;
    }
  } catch (const std::exception &e) {
    std::cerr << e.what() << std::endl;
    return 1;
  }

  dbconn->prepare("INSERT INTO workers DEFAULT VALUES");
  dbconn->disconnect();

}