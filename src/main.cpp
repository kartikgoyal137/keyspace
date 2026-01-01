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
#include <deque>
#include <unordered_map>
#include <shared_mutex>
#include <condition_variable>

std::map <std::string, std::string> store;
std::unordered_map<std::string, std::deque<std::string>> lists;
std::shared_mutex sm;
std::condition_variable_any cv;

std::string arr_to_resp(std::vector<std::string> array) {
  std::string response;
  int64_t size = array.size();  
  response = "*"+std::to_string(size)+"\r\n";
  for(auto& ele : array) {
    response += "$"+std::to_string(ele.size())+"\r\n"+ele+"\r\n";
  }

  return response;
}

void handle_key_expiry(std::string key, int64_t millisec) {
  std::this_thread::sleep_for(std::chrono::milliseconds(millisec));
  {
    std::unique_lock<std::shared_mutex> lock(sm);
    if(store.find(key)!=store.end()) store.erase(key);
  }
}

std::string PING() {
  return "+PONG\r\n";
}

std::string ECHO(std::string text) {
  return "$"+std::to_string(text.length())+"\r\n"+text+"\r\n";
}

std::string SET(std::vector<std::string>& command) {
  std::string key = command[1]; std::string val = command[2];
  {
    std::unique_lock<std::shared_mutex> lock(sm);
    store[key]=val;
  }

  if(command.size()>4) {
    int64_t expire_time = std::stoi(command[4]);
    if(command[3]=="EX") {
      std::thread t(handle_key_expiry, key, expire_time*1000);
      t.detach();
    }
    else if (command[3]=="PX") {
      std::thread t(handle_key_expiry, key, expire_time);
      t.detach();
    }
  }

  return "+OK\r\n";

}

std::string GET(std::string key) {
  std::string val;
   if(store.find(key)!=store.end()) {
    {
      std::shared_lock<std::shared_mutex> lock(sm);
      val = store[key];

    }
      return "$"+std::to_string(val.length())+"\r\n"+val+"\r\n";
   }

   return "$-1\r\n";
}

std::string RPUSH(std::vector<std::string>& command) {
   std::string list_key = command[1];
   auto& dq = lists[list_key];

   {
   std::unique_lock<std::shared_mutex> lock(sm);

   for(int i=2; i<command.size(); i++) {
     std::string element = command[i];
     dq.push_back(element);
     }
   }
    cv.notify_one();
    return ":"+std::to_string(dq.size())+"\r\n";
}

std::string LPUSH(std::vector<std::string>& command) {
   std::string list_key = command[1];
        auto& dq = lists[list_key];
   {
     std::unique_lock<std::shared_mutex> lock(sm);

     for(int i=2; i<command.size(); i++) {
       std::string element = command[i];
       dq.push_front(element);
     }

   }
    cv.notify_one();
        
    return ":"+std::to_string(dq.size())+"\r\n";
}


std::string LRANGE(std::string list_key, int64_t start, int64_t stop) {
  std::vector<std::string> array;
  {
    std::shared_lock<std::shared_mutex> lock(sm);
    int64_t size = lists[list_key].size();
    if(lists.find(list_key)==lists.end() || start >= size || start > stop) return arr_to_resp(array);
    if(stop >= size) stop = size-1;

    for(int i=start; i<=stop; i++) {
      array.push_back(lists[list_key][i]);
    }
  }


  return arr_to_resp(array);
}

std::string LLEN(std::string list_key) {
  int64_t size = 0;
  {
    std::shared_lock<std::shared_mutex> lock(sm);
    size = lists[list_key].size();
  }
  std::string response = ":"+std::to_string(size)+"\r\n";
  return response;
}

std::string LPOP(std::string list_key, int64_t num) { //add mutex
  auto& dq = lists[list_key];
  std::vector<std::string> array;
  if(dq.size()>0) {
    std::string text = dq.front(); dq.pop_front();
    array.push_back(text);
    if(num==1) return "$"+std::to_string(text.length())+"\r\n"+text+"\r\n";
    num--;
    while(num--) {
      text = dq[0]; dq.pop_front();
      array.push_back(text);
    }
    return arr_to_resp(array);
  }
  else return "$-1\r\n";
}

std::string BLPOP(std::string list_key, int64_t timeout) {
  std::vector<std::string> array; array.push_back(list_key);
  bool ready = true;

    {
      std::unique_lock<std::shared_mutex> lock(sm);

      if(timeout==0) {
         cv.wait(lock, [&] {
         return lists[list_key].size()>0;
         });
         auto& dq = lists[list_key];
         std::string text = dq.front(); dq.pop_front();
         array.push_back(text);
      }
      else {
         ready = cv.wait_for(lock, std::chrono::seconds(timeout), [&] {
        return lists[list_key].size()>0;
      });
        if(ready) {
         auto& dq = lists[list_key];
         std::string text = dq.front(); dq.pop_front();
         array.push_back(text);
         ready=true;
      }
      }
    }

  if(!ready) return "$-1\r\n";

  return arr_to_resp(array);
}

void handle_command(int client_fd, std::vector<std::string>& command) {
  if(command.size()>0) {
    std::string cmd = command[0];
    std::transform(cmd.begin(), cmd.end(), cmd.begin(), ::toupper);
    std::string response = "$-1\r\n";

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
        int64_t start = std::stoll(command[2]);
        int64_t stop = std::stoll(command[3]);
        int64_t size = lists[command[1]].size();
        if(start<0) {
          if(-start>size) start = 0;
          else start += size;
        }
        if(stop<0) {
          if(-stop>size) stop = 0;
          else stop += size;
        }
        response = LRANGE(command[1], start, stop);
      }
    }
    else if(cmd=="LLEN") {
      if(command.size()>1) {
        response = LLEN(command[1]);
      }
    }
    else if(cmd=="LPOP") {
      if(command.size()>1) {
        int64_t num = 1;
        if(command.size()>2) num = std::stoll(command[2]);
        response = LPOP(command[1], num);
      }
    }
    else if(cmd=="BLPOP") {
      if(command.size()>2) {
        int64_t timeout = std::stoll(command[2]);
        response = BLPOP(command[1], timeout);
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
