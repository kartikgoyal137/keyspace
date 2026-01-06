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
    else if(cmd=="CONFIG") {
      response = CONFIG(command);
    }
    else if(cmd=="INFO") {
      response = INFO(command);
    }
    else if(cmd=="REPLCONF") {
      response= REPLCONF();
    }
    else if(cmd=="PSYNC") {
      response = PSYNC();
      send(client_fd, response.c_str(), response.size(), 0);
      const unsigned char emptyRDB[] = {
            0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31, 0xfa, 0x09, 0x72, 0x65, 0x64, 0x69, 0x73,
            0x2d, 0x76, 0x65, 0x72, 0x05, 0x37, 0x2e, 0x32, 0x2e, 0x30, 0xfa, 0x0a, 0x72, 0x65, 0x64, 0x69,
            0x73, 0x2d, 0x62, 0x69, 0x74, 0x73, 0xc0, 0x40, 0xfa, 0x05, 0x63, 0x74, 0x69, 0x6d, 0x65, 0xc2,
            0x6d, 0x08, 0xbc, 0x65, 0xfa, 0x08, 0x75, 0x73, 0x65, 0x64, 0x2d, 0x6d, 0x65, 0x6d, 0xc2, 0xb0,
            0xc4, 0x10, 0x00, 0xfa, 0x08, 0x61, 0x6f, 0x66, 0x2d, 0x62, 0x61, 0x73, 0x65, 0xc0, 0x00, 0xff,
            0xf0, 0x6e, 0x3b, 0xfe, 0xc0, 0xff, 0x5a, 0xa2
          };
      response = "$" + std::to_string(sizeof(emptyRDB)) + "\r\n";
      send(client_fd, response.c_str(), response.size(), 0);
      send(client_fd, emptyRDB, sizeof(emptyRDB), 0);
      return;
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
  
   int master_port = -1;
   std::string master_host="-1";

   for(int i=1; i<argc; i++) {
    std::string arg = argv[i];
    if(arg=="--dir" && i+1<argc) dir = argv[++i];
    else if(arg=="--dbfilename" && i+1<argc) dbfilename = argv[++i];
    else if(arg=="--port" && i+1<argc){
      port_number = std::stoi(argv[++i]); 
    }
    else if(arg=="--replicaof") {
      Server& server = server_info[port_number];
      server.role = "slave";
      std::string master = argv[++i];
      int index = master.find(" ");
      
      master_host = master.substr(0, index);
      if(master_host=="localhost") master_host = "127.0.0.1";
      master_port = std::stoi(master.substr(index+1));
    }
  }

  Server& server = server_info[port_number];
  if(server.role=="-1"){
    server.role = "master";
    server.master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
    server.master_repl_offset = "0";
  }
  else if(server.role=="slave") {
    int master_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (master_fd < 0) {
     std::cerr << "Failed to create master socket\n";
     return 1;
    }
    
    struct sockaddr_in master_addr;
    master_addr.sin_family = AF_INET;
    master_addr.sin_port = htons(master_port);

    inet_pton(AF_INET, master_host.c_str(), &master_addr.sin_addr);

    if(connect(master_fd, (const sockaddr*) &master_addr, sizeof(master_addr)) < 0) {
     std::cerr << "failed to connect to master\n";
     return 1;
    }

    const char* ping = "*1\r\n$4\r\nPING\r\n";
    const char* replconf2 = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
    const char* replconf1 = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n";
    const char* psync = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
    char buffer[1024];
    send(master_fd, ping, strlen(ping), 0);
    ssize_t bytes = recv(master_fd, buffer, sizeof(buffer)-1, 0);
    if(bytes==-1) {
      std::cerr << "PING to master failed";
      return 1;
    }

    send(master_fd, replconf1, strlen(replconf1), 0);
     bytes = recv(master_fd, buffer, sizeof(buffer)-1, 0);
    send(master_fd, replconf2, strlen(replconf2), 0);
     bytes = recv(master_fd, buffer, sizeof(buffer)-1, 0);
    send(master_fd, psync, strlen(psync), 0);
     bytes = recv(master_fd, buffer, sizeof(buffer)-1, 0);
      
    std::string rdb;

    while(true) {
      bytes = recv(master_fd, buffer, sizeof(buffer), 0);
      if(bytes==0) {
        std::cout << "Connection closed\n";
        break;
      }
      else if(bytes<0) {
        std::cerr << "Error while recieving\n";
        break;
       }
         
      rdb.append(buffer, bytes);      
    }

  }




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
  server_addr.sin_port = htons(port_number);
  
  if (bind(server_fd, (struct sockaddr *) &server_addr, sizeof(server_addr)) != 0) {
    std::cerr << "Failed to bind to port "+std::to_string(port_number)+"\n";
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
