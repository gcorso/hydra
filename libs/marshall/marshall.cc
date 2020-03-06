#include <marshall/marshall.h>
#include <db/db.h>
#include <log.h>

MAKE_STREAM_STRUCT(std::cerr, "marshall: ", marshall)


namespace hydra::marshall{

void marshall(const uint64_t execution_id) {
  db::execution execution(execution_id);
  log::marshall << "executing command \"" << execution.command << "\""<<std::endl;
}
}