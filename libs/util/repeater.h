#ifndef HYDRA_LIBS_UTIL_REPEATER_H_
#define HYDRA_LIBS_UTIL_REPEATER_H_
#include <future>
namespace hydra {
class repeater {
 public:
  repeater() {};
  //template<typename Fun>
  //repeater(Fun f, std::chrono::milliseconds delay) : active_(false), method_(f), delay_(delay) {}
  ~repeater() { if(joinable())std::terminate(); }
  template<typename _Callable, typename... _Args>
  void run(_Callable &&fun, std::chrono::milliseconds delay, _Args &&... args) {
    p_ = std::promise<void>();
    if (thread_.joinable()) std::terminate;
    thread_ = std::thread([f = p_.get_future(),delay,fun,args...]() mutable -> void {
      while (true) {
        if(f.wait_for(delay) == std::future_status::ready)break;
        std::invoke(std::forward<_Callable&&>(fun),std::forward<_Args&&>(args)...);
      }
    });
  }
  void kill() {
    p_.set_value();
    thread_.join();
  }
  bool joinable() {
    return thread_.joinable();
  }
 private:
  std::thread thread_;
  std::promise<void> p_;
};
}
#endif //HYDRA_LIBS_UTIL_REPEATER_H_
