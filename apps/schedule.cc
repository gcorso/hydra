#include <db/db.h>
#include <iostream>
#include <algorithm>

std::string_view trim(std::string_view s){
  while(!s.empty() && s.front()<=32)s.remove_prefix(1);
  while(!s.empty() && s.back()<=32)s.remove_suffix(1);
  return s;
}

int main(){
  for (std::string line; std::getline(std::cin, line);) {
    std::string_view command = trim(line);
    if(command.empty())continue;
    if(command.front()=='#')continue;
    if(std::find(command.begin(),command.end(),'\'')!=command.end())throw;
    std::cout<< "Scheduling: " << '"' << command << std::endl << '"' << "... ";
    std::string cmd = std::string("INSERT INTO jobs (command,environment_id,checkpoint_policy_id,state_id) VALUES ('").append(command).append("',219,1,2);");
    //std::cout << cmd << std::endl;
    hydra::db::execute_command(cmd);
    std::cout << "done!" << std::endl;
  }
  return 0;
}
