#include <db/db.h>
#include <iostream>
#include <algorithm>

int main(){
  for (std::string line; std::getline(std::cin, line);) {
    if(line.empty())continue;
    line.pop_back();
    if(line.empty())continue;
    if(std::find(line.begin(),line.end(),'#')!=line.end())continue;
    if(std::find(line.begin(),line.end(),'\'')!=line.end())throw;
    std::string cmd = std::string("INSERT INTO jobs (command,environment_id,checkpoint_policy_id,state_id) VALUES ('").append(line).append("',219,1,2);");
    std::cout << cmd << std::endl;
    hydra::db::execute_command(cmd);
  }
  return 0;
}
