#include <db/db.h>
#include <cstdlib>
#include <log.h>

MAKE_STREAM_STRUCT(std::cerr,"db: ",db);

namespace hydra::db{

namespace helper{
struct disconnector{
  disconnector() = default;
  ~disconnector(){
    log::db << "destroying disconnector" << std::endl;
    disconnect();
  }
};
disconnector db_disconnector;
}

PGconn *conn = nullptr;

void connect(){
  if(conn)return;
  conn = PQconnectdb(HYDRA_LIBS_DB_CONNECTION_STRING);
  if (PQstatus(conn) != CONNECTION_OK) {
    log::fatal << "connection to database failed: " << PQerrorMessage(conn);
    PQfinish(conn);
    exit(1);
  }
  log::db << "connected to database" << std::endl;
}

void disconnect(){
  if(conn==nullptr)return;
  PQfinish(conn);
  conn=nullptr;
  log::db << "disconnected from database" << std::endl;
}


}