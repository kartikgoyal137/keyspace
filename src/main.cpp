#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <thread>
#include <vector>
#include <map>
#include <algorithm>
#include <chrono>

using Clock = std::chrono::steady_clock;

std::map <std::string, std::string> mp;

void handle_key_expiry(string key, int millisec) {
  std::this_thread::sleep_for(std::chrono::milliseconds(millisec));
  if(mp.find(key)!=mp.end()) mp.erase(key);
}

void handle_command(int client_fd, std::vector<std::string>& command) {
  if(command.size()>0) {
    std::string cmd = command[0];
    std::transform(cmd.begin(), cmd.end(), cmd.begin(), ::toupper);
    std::string response = "$-1\r\n";

    if(cmd=="PING") {
      response = "+PONG\r\n";
    }
    else if (cmd=="ECHO") {
      if(command.size()>1) {
        std::string text = command[1];
        response = "$"+std::to_string(text.length())+"\r\n"+text+"\r\n";
      }
    }
    else if(cmd=="SET") {
      if(command.size()>2) {
        std::string key = command[1]; std::string val = command[2];
          mp[key]=val;
          response = "+OK\r\n";
        if(command.size()>4) {
          int expire_time = std::stoi(command[4]);
          if(command[3]=="EX") {
            std::thread t(handle_key_expiry, key, command[4]*1000);
            t.detach();
          }
          else if (command[3]=="PX") {
            std::thread t(handle_key_expiry, key, command[4]);
            t.detach();
          }
        }
      }
    }
    else if(cmd=="GET") {
        if(command.size()>1) {
        std::string key = command[1];
        if(mp.find(key)!=mp.end()) {
          std::string val = mp[key];
          response = "$"+std::to_string(val.length())+"\r\n"+val+"\r\n";
        }
      }

    }

    send(client_fd, response.c_str(), response.length(), 0);

  }

}

std::vector<std::string> resp_parser(int client_fd, const char* buffer) {
  std::vector<std::string> args; int pos = 0;
  if(buffer[pos]!='*') return args;
  pos++; int tokens = 0;
  while(buffer[pos]!='\r') {
    tokens = tokens*10 + (buffer[pos]-'0');
    pos++;
  }

  pos+=2;

  for(int i=0; i<tokens; i++) {
    if(buffer[pos]!='$') break;
    pos++; int str_len = 0;
    while(buffer[pos]!='\r') {
      str_len = str_len*10 + (buffer[pos]-'0'); 
      pos++;
    }
    pos+=2;

    std::string arg(buffer+pos, str_len);
    args.push_back(arg);
    pos+= str_len+2;
  }

  return args;
}

void handle_client(int client_fd) {
   std::cout << "Client connected\n";
  char buffer[1024] = {0};
  const char* p = "+PONG\r\n";
  while(true) {
    memset(buffer, 0, sizeof(buffer));
    int bytes = recv(client_fd, buffer, sizeof(buffer)-1, 0);
    if(bytes==0) {
      std::cout << "Connection closed\n";
      break;
    }
    else if(bytes<0) {
      std::cerr << "Error while recieving\n";
      break;
    }
    
    std::vector<std::string> command = resp_parser(client_fd, buffer);
    handle_command(client_fd, command);
  }
  // 
   close(client_fd);
}


int main(int argc, char **argv) {
  // Flush after every std::cout / std::cerr
  std::cout << std::unitbuf;
  std::cerr << std::unitbuf;
  
  int server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd < 0) {
   std::cerr << "Failed to create server socket\n";
   return 1;
  }
  
  // Since the tester restarts your program quite often, setting SO_REUSEADDR
  // ensures that we don't run into 'Address already in use' errors
  int reuse = 1;
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
    std::cerr << "setsockopt failed\n";
    return 1;
  }
  
  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = INADDR_ANY;
  server_addr.sin_port = htons(6379);
  
  if (bind(server_fd, (struct sockaddr *) &server_addr, sizeof(server_addr)) != 0) {
    std::cerr << "Failed to bind to port 6379\n";
    return 1;
  }
  
  int connection_backlog = 5;
  if (listen(server_fd, connection_backlog) != 0) {
    std::cerr << "listen failed\n";
    return 1;
  }

  while(true) {
  
  struct sockaddr_in client_addr;
  socklen_t client_addr_len = sizeof(client_addr);
  std::cout << "Waiting for a client to connect...\n";

  // You can use print statements as follows for debugging, they'll be visible when running tests.
  std::cout << "Logs from your program will appear here!\n";

  // Uncomment the code below to pass the first stage
  int client_fd = accept(server_fd, (struct sockaddr *) &client_addr, &client_addr_len);
  if(client_fd<0) {
      std::cerr << "Accept failed\n"; continue;
    }

  std::thread t(handle_client, client_fd);
  t.detach();
  }


  close(server_fd);

  return 0;
}
