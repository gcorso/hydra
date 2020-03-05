#include <stdio.h>
#include <stdlib.h>
#include <postgres/libpq-fe.h>
#include <sys/stat.h>
#include <cerrno>
#include <iostream>
static void
exit_nicely(PGconn *conn) {
  PQfinish(conn);
  exit(1);
}

int
main2(int argc, char **argv) {
  const char *conninfo;
  PGconn *conn;
  PGresult *res;
  int nFields;
  int i,
      j;

  /*
   * If the user supplies a parameter on the command line, use it as the
   * conninfo string; otherwise default to setting dbname=postgres and using
   * environment variables or defaults for all other connection parameters.
   */
    conninfo = "host = database-3.cydtzfbe17ky.us-east-1.rds.amazonaws.com password = enmNY9aKTvv7PE2lFSF2 user = postgres port = 5432";

  /* Make a connection to the database */
  conn = PQconnectdb(conninfo);

  /* Check to see that the backend connection was successfully made */
  if (PQstatus(conn) != CONNECTION_OK) {
    fprintf(stderr, "Connection to database failed: %s",
            PQerrorMessage(conn));
    exit_nicely(conn);
  }

  /*
   * Our test case here involves using a cursor, for which we must be inside
   * a transaction block.  We could do the whole thing with a single
   * PQexec() of "select * from pg_database", but that's too trivial to make
   * a good example.
   */

  /* Start a transaction block */
  res = PQexec(conn, "BEGIN");
  if (PQresultStatus(res) != PGRES_COMMAND_OK) {
    fprintf(stderr, "BEGIN command failed: %s", PQerrorMessage(conn));
    PQclear(res);
    exit_nicely(conn);
  }

  /*
   * Should PQclear PGresult whenever it is no longer needed to avoid memory
   * leaks
   */
  PQclear(res);

  /*
   * Fetch rows from pg_database, the system catalog of databases
   */
  res = PQexec(conn, "DECLARE myportal CURSOR FOR select * from pg_database");
  if (PQresultStatus(res) != PGRES_COMMAND_OK) {
    fprintf(stderr, "DECLARE CURSOR failed: %s", PQerrorMessage(conn));
    PQclear(res);
    exit_nicely(conn);
  }
  PQclear(res);

  res = PQexec(conn, "FETCH ALL in myportal");
  if (PQresultStatus(res) != PGRES_TUPLES_OK) {
    fprintf(stderr, "FETCH ALL failed: %s", PQerrorMessage(conn));
    PQclear(res);
    exit_nicely(conn);
  }

  /* first, print out the attribute names */
  nFields = PQnfields(res);
  for (i = 0; i < nFields; i++)
    printf("%-15s", PQfname(res, i));
  printf("\n\n");

  /* next, print out the rows */
  for (i = 0; i < PQntuples(res); i++) {
    for (j = 0; j < nFields; j++)
      printf("%-15s", PQgetvalue(res, i, j));
    printf("\n");
  }

  PQclear(res);

  /* close the portal ... we don't bother to check for errors ... */
  res = PQexec(conn, "CLOSE myportal");
  PQclear(res);

  /* end the transaction */
  res = PQexec(conn, "END");
  PQclear(res);

  /* close the connection to the database and cleanup */
  PQfinish(conn);

  return 0;
}

int main(){
  /*
  std::cout << getenv("HOME") << std::endl;
  if(mkdir("/home/luke/.sample",777)){
    std::cerr << "could not create worker directory in  "<<"~/.sample" << "(errno: "<<errno<<")"<<std::endl;
    exit(1);
  }*/
  FILE *fp = fopen("/home/luke/.worker/id","wb");
  if (!fp){
    std::cerr<< "could not store worker id in " << "(errno: "<<errno<<")"<<std::endl;
    exit(1);
  }
}