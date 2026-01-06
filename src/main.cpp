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
#include "functions.h"

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


void handle_command(int client_fd, std::vector<std::string> command) {
  std::string response = "$-1\r\n";
  if(command.size()>0) {
    std::string cmd = command[0];
    std::transform(cmd.begin(), cmd.end(), cmd.begin(), ::toupper);
    

    if(command[0]!="EXEC" && command[0]!="DISCARD") {
      response = QUEUE(command, client_fd);
      if(response!="-1") {
        send(client_fd, response.c_str(), response.size(), 0);
        return;
      }
    }

    if(cmd=="PING") {
      response = PING();
}
    else if (cmd=="ECHO") {
      if(command.size()>1) {
         response = ECHO(command[1]);
      }
    }
    else if(cmd=="SET") {
      if(command.size()>2) {
        response = SET(command);
      }
    }
    else if(cmd=="GET") {
      if(command.size()>1) {
        response = GET(command[1]);       
      }
    }
    else if(cmd=="RPUSH") {
      if(command.size()>2) {
        response = RPUSH(command); 
      }
    }
    else if(cmd=="LPUSH") {
      if(command.size()>2) {
        response = LPUSH(command); 
      }
    }
    else if(cmd=="LRANGE") { 
      if(command.size()>3) {
        response = LRANGE(command);
      }
    }
    else if(cmd=="LLEN") {
      if(command.size()>1) {
        response = LLEN(command[1]);
      }
    }
    else if(cmd=="LPOP") {
      if(command.size()>1) {
        response = LPOP(command);
      }
    }
    else if(cmd=="BLPOP") {
      if(command.size()>2) {
        response = BLPOP(command);
      }
    }
    else if(cmd=="TYPE") {
      if(command.size()>1) {
  response = TYPE(command[1]);
      }
    }
    else if(cmd=="XADD") {
      if(command.size()>2) {
        response = XADD(command);
      }
    }
    else if(cmd=="XRANGE") {
      if(command.size()>3) {
        response = XRANGE(command);
      }
    }
    else if(cmd=="XREAD") {
      if(command.size()>3) {
        response = XREAD(command);
      }
    }
    else if(cmd=="INCR") {
      if(command.size()>1) {
        response = INCR(command[1]);
      }
    }
    else if(cmd=="MULTI") {
      response = MULTI(client_fd);
    }
    else if(cmd=="EXEC") {
      response = EXEC(client_fd);
    }
    else if(cmd=="DISCARD") {
      response = DISCARD(client_fd);
    }

  
  }

   send(client_fd, response.c_str(), response.size(), 0); 

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
