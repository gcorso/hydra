#ifndef HYDRA_LIBS_ENV_MANAGER_ENV_MANAGER_H_
#define HYDRA_LIBS_ENV_MANAGER_ENV_MANAGER_H_
#include <unordered_map>
#include <zconf.h>
#include <sys/stat.h>
#include <log/log.h>
#include <fcntl.h>
#include <util/util.h>
#include <db/db.h>
#include <util/executor.h>

namespace hydra {
using util::strjoin;

class env_manager {
 public:
  // blocking
  static int make_environment(int id) {

    if (id == 0) {
      rmdir("/tmp/__hydraenv_0__");
      if (mkdir("/tmp/__hydraenv_0__", 0777)) {
        log::fatal << "could not create directory: /tmp/__hydraenv_0__; errno: " << errno << std::endl;
        exit(1);
      }
      return open("/tmp/__hydraenv_0__",O_RDONLY | O_DIRECTORY);
    }

    if(auto it = envs.find(id); it!=envs.end()){
      system(strjoin("git -C /tmp/__hydraenv_", id, "__ pull -p;").c_str());
      return it->second;
    }
    else return envs[id] = create_environment(id);
  }
 private:
 static  std::unordered_map<int,int> envs; //id to folder fd

 static int create_environment(int id){
   if (mkdir(strjoin("/tmp/__hydraenv_", id, "__").c_str(), 0777) && errno != EEXIST) {
     log::fatal << "could not create directory: /tmp/__hydraenv_" << id << "__; errno: " << errno << std::endl;
     exit(1);
   }

   std::string gitlink = db::awsdb.single_result_query(strjoin("SELECT gitlink FROM environments WHERE id = ", id));
   system(strjoin("git clone ", gitlink, " /tmp/__hydraenv_", id, "__; git -C /tmp/__hydraenv_", id, "__ pull -p; git -C /tmp/__hydraenv_", id, "__ clean -f -d;").c_str());

   std::string setup = db::awsdb.single_result_query(strjoin("SELECT bash_setup FROM environments WHERE id = ", id));

   int fd = open(strjoin("/tmp/__hydraenv_",id,"__").c_str(),O_RDONLY | O_DIRECTORY);
   if(!setup.empty()){
     using namespace util;
     const executor::termination t = util::executor().run(setup.c_str(),{.execution_directory = fd},[](){}).join();
     if(!std::holds_alternative<util::executor::returned>(t) || std::get<util::executor::returned>(t).exit_code!=0){
      log::fatal << "bad setup: "<<t<<std::endl;
       exit(1);
     }
   }
   return fd;
 }

};

}

#endif //HYDRA_LIBS_ENV_MANAGER_ENV_MANAGER_H_
