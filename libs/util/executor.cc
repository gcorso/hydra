#include "executor.h"

namespace hydra::util {

std::ostream &operator<<(std::ostream &o, const hydra::util::executor::returned &x) {
  o << "Process finished with exit code " << x.exit_code;
  return o;
}

std::ostream &operator<<(std::ostream &o, const executor::signaled &x) {
  o << "Process terminated with signal " << x.signal;
  return o;
}
std::ostream &operator<<(std::ostream &o, const executor::core_dumped &x) {
  o << "Segmentation fault (core dumped)";
  return o;
}
std::ostream &operator<<(std::ostream &o, const executor::termination &x) {
  std::visit([&o](const auto& v) { o << v; }, x);
  return o;
}
}
