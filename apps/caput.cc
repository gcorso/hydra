#include <worker/worker.h>

int main() {
  hydra::worker w(hydra::worker::params{.execution_directory = 45}, []() { std::cerr << "worker ended" << std::endl; });
  std::string cmd;
  while (true) {
    std::getline(std::cin, cmd);
    if (cmd == "q" || cmd == "quit") {
      //w.kill();
      break;
    }
  }
  //w.start()

}