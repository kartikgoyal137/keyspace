#ifndef FUNCTIONS_H
#define FUNCTIONS_H

#include <algorithm>
#include <chrono>
#include <deque>
#include <unordered_map>
#include <shared_mutex>
#include <condition_variable>
#include <string>
#include <thread>
#include <map>
#include <set>
#include <vector>

enum class KeyType {
  string,
  list,
  stream
};

struct StreamEntry {
  std::unordered_map<std::string, std::string> fields;
};

struct Stream {
    std::map<std::string, StreamEntry> entries;
};

extern std::string dir;
extern std::string dbfilename;
extern int port_number;


extern std::set<int> pending_fd; 
extern std::map<int, std::vector<std::vector<std::string>>> pending_cmd;
extern std::unordered_map<std::string, KeyType> key_type;
extern std::unordered_map <std::string, std::string> store;
extern std::unordered_map<std::string, std::deque<std::string>> lists;
extern std::unordered_map<std::string, Stream> streams;

extern std::shared_mutex sm_keytype;
extern std::shared_mutex sm_store;
extern std::shared_mutex sm_list;
extern std::shared_mutex sm_stream;
extern std::shared_mutex sm_fd;
extern std::condition_variable_any list_cv;
extern std::condition_variable_any stream_cv;

void handle_key_expiry(std::string key, int64_t millisec, std::string val);
std::string PING();
std::string ECHO(std::string text);
std::string SET(std::vector<std::string>& command);
std::string GET(std::string key);
std::string RPUSH(std::vector<std::string>& command);
std::string LPUSH(std::vector<std::string>& command);
std::string LRANGE(std::vector<std::string> command);
std::string LLEN(std::string list_key);
std::string LPOP(std::vector<std::string> command);
std::string BLPOP(std::vector<std::string> command);
std::string enum_to_string(KeyType t);
std::string TYPE(std::string key);
std::string XADD(std::vector<std::string> command);
std::string XRANGE(std::vector<std::string> command);
std::string XREAD(std::vector<std::string> command);
std::string INCR(std::string store_key);
std::string MULTI(int fd);
std::string call_function(std::vector<std::string> command, int client_fd);
std::string EXEC(int fd);
std::string QUEUE(std::vector<std::string> command, int fd);
std::string DISCARD(int fd);
std::string CONFIG(std::vector<std::string> command);

#endif


