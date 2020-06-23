#include <worker/worker.h>
#include <chrono>
using namespace std::chrono_literals;

int main() {
  hydra::worker w({.idleness_limit = 4, .wait_delay=6s}, []() { std::cerr << "worker ended" << std::endl; exit(0); });
  w.run();
  std::string cmd;
  while (true) {
    std::getline(std::cin, cmd);
    std::cerr << "ENTERED COMMAND: " << cmd << std::endl;
    if (cmd == "q" || cmd == "quit") {
      w.kill();
      break;
    } else if (cmd == "w" || cmd == "wait") {
      w.join();
      break;

    }
  }
  //w.start()

}