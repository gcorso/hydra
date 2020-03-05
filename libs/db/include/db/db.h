#ifndef HYDRA_LIBS_DB_INCLUDE_DB_DB_H_
#define HYDRA_LIBS_DB_INCLUDE_DB_DB_H_
#include "config.h"

namespace hydra::db{

extern PGconn *conn;
void connect();
void disconnect();

}

#endif //HYDRA_LIBS_DB_INCLUDE_DB_DB_H_
