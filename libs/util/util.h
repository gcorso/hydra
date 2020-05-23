#ifndef HYDRA_LIBS_UTIL_UTIL_H_
#define HYDRA_LIBS_UTIL_UTIL_H_

#include <cstdlib>
#include <sstream>

namespace hydra::util {
namespace {
template<typename ...Args>
struct joiner;
template<>
struct joiner<> {
  static inline void append_to(std::stringstream &cref) { return; }
};
template<typename T, typename...Ts>
struct joiner<T, Ts...> {
  static inline void append_to(std::stringstream &cref, const T &t, Ts &&... ts) {
    cref << t;
    joiner<Ts...>::append_to(cref, std::forward<Ts>(ts)...);
  }
};

}

template<typename ...Ts>
std::string strjoin(Ts &&... ts) {
  std::stringstream ss;
  util::joiner<Ts...>::append_to(ss, std::forward<Ts>(ts)...);
  return ss.str();
}
}

#endif //HYDRA_LIBS_UTIL_UTIL_H_
