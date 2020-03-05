#ifndef HYDRA_LIBS_LOG_INCLUDE_LOG_H_
#define HYDRA_LIBS_LOG_INCLUDE_LOG_H_
#include <iostream>

namespace hydra::log{

#define STATUS_MACROS_CONCAT_NAME_INNER(x, y) x##y
#define STATUS_MACROS_CONCAT_NAME(x, y) STATUS_MACROS_CONCAT_NAME_INNER(x, y)



#define MAKE_STREAM_STRUCT_N(stream,preamble_content,name,structname) struct structname {\
  static constexpr std::string_view preamble = preamble_content; \
  template<typename T>\
  inline auto& operator<<(const T& value){\
    return stream << preamble << value;\
  }\
};\
inline structname name;

#define MAKE_STREAM_STRUCT(stream,preamble,name) MAKE_STREAM_STRUCT_N(stream,preamble,name,STATUS_MACROS_CONCAT_NAME(__stream_struct_, __COUNTER__))


MAKE_STREAM_STRUCT(std::cerr,"fatal: ",fatal)
MAKE_STREAM_STRUCT(std::cerr,"worker: ",worker)


}

#endif //HYDRA_LIBS_LOG_INCLUDE_LOG_H_
