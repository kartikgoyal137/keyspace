#include <algorithm>
#include <chrono>
#include <deque>
#include <unordered_map>
#include <shared_mutex>
#include <condition_variable>
#include <string>
#include <thread>
#include <map>
#include <unordered_map>
#include "utils.h"

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

std::unordered_map<std::string, KeyType> key_type;
std::unordered_map <std::string, std::string> store;
std::unordered_map<std::string, std::deque<std::string>> lists;
std::unordered_map<std::string, Stream> streams;

std::shared_mutex sm_keytype;
std::shared_mutex sm_store;
std::shared_mutex sm_list;
std::shared_mutex sm_stream;
std::condition_variable_any list_cv;
std::condition_variable_any stream_cv;

void handle_key_expiry(std::string key, int64_t millisec, std::string val) {
  std::this_thread::sleep_for(std::chrono::milliseconds(millisec));
 {
    std::unique_lock<std::shared_mutex> lock(sm_store);
    if(store.find(key)!=store.end() && val==store[key]) {
      store.erase(key); 
      {
        std::unique_lock<std::shared_mutex> typelock(sm_keytype);
        key_type.erase(key);
      }

    }
  }
}


std::string PING() {
  return "+PONG\r\n";
}

std::string ECHO(std::string text) {
  return bulk_string(text);
}

std::string SET(std::vector<std::string>& command) {
  std::string key = command[1]; std::string val = command[2];
  {
    std::unique_lock<std::shared_mutex> lock(sm_store);
    store[key]=val;
      {
        std::unique_lock<std::shared_mutex> typelock(sm_keytype);
        key_type[key] = KeyType::string;

      }

  }

  if(command.size()>4) {
    int64_t expire_time = std::stoi(command[4]);
    if(command[3]=="EX") {
      std::thread t(handle_key_expiry, key, expire_time*1000, val);
      t.detach();
    }
    else if (command[3]=="PX") {
      std::thread t(handle_key_expiry, key, expire_time, val);
      t.detach();
    }
  }

  return "+OK\r\n";

}

std::string GET(std::string key) {
  std::string val; std::string response = "$-1\r\n";

    {
      std::shared_lock<std::shared_mutex> lock(sm_store);
      if(store.find(key)!=store.end()) {
      val = store[key];
      response = bulk_string(val);
    }
    }


   return response;
}

std::string RPUSH(std::vector<std::string>& command) {
   std::string list_key = command[1]; int size = 0;

   {
   std::unique_lock<std::shared_mutex> lock(sm_list);

   if(lists.find(list_key)==lists.end()) {
      {
        std::unique_lock<std::shared_mutex> typelock(sm_keytype);
        key_type[list_key] = KeyType::list;

      }

    }
   auto& dq = lists[list_key];

   for(int i=2; i<command.size(); i++) {
     std::string element = command[i];
     dq.push_back(element);
     }

   size = dq.size();
   }

    list_cv.notify_one();
    return ":"+std::to_string(size)+"\r\n";
}

std::string LPUSH(std::vector<std::string>& command) {
   std::string list_key = command[1];
    int size = 0;
   {
     std::unique_lock<std::shared_mutex> lock(sm_list);
    
   auto& dq = lists[list_key];
     if(lists.find(list_key)==lists.end()) {
      {
        std::unique_lock<std::shared_mutex> typelock(sm_keytype);
        key_type[list_key] = KeyType::list;

      }

    }


     for(int i=2; i<command.size(); i++) {
       std::string element = command[i];
       dq.push_front(element);
     }
    size = dq.size();

   }
    list_cv.notify_one();
        
    return ":"+std::to_string(size)+"\r\n";
}


std::string LRANGE(std::string list_key, int64_t start, int64_t stop) {
  std::vector<std::string> array;
  {
    std::shared_lock<std::shared_mutex> lock(sm_list);
    int64_t size = lists[list_key].size();
        if(start<0) {
          if(-start>size) start = 0;
          else start += size;
        }
        if(stop<0) {
          if(-stop>size) stop = 0;
          else stop += size;
        }

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
    std::shared_lock<std::shared_mutex> lock(sm_list);
    size = lists[list_key].size();
  }
  std::string response = ":"+std::to_string(size)+"\r\n";
  return response;
}

std::string LPOP(std::string list_key, int64_t num) { 
  std::string response = "-1";
  std::vector<std::string> array;

  {
    std::unique_lock<std::shared_mutex> lock(sm_list);
    auto& dq = lists[list_key];
    if(num > dq.size()) num = dq.size();

    if(dq.size()>0 && !dq.empty()) {
      std::string text = dq.front(); dq.pop_front();
      array.push_back(text);
      if(num==1) response = bulk_string(text);
        num--;
        while(num--) {
        text = dq[0]; dq.pop_front();
        array.push_back(text);
        }

      if(response=="-1") response = arr_to_resp(array);
    }
    else response = "$-1\r\n";

      if(lists[list_key].size()==0) {
    lists.erase(list_key); 
    {
      std::unique_lock<std::shared_mutex> typelock(sm_keytype);
      key_type.erase(list_key);
    }
  }
  }



  return response;
}

std::string BLPOP(std::string list_key, int64_t timeout) {
  std::vector<std::string> array; array.push_back(list_key);
  bool ready = true;

    {
      std::unique_lock<std::shared_mutex> lock(sm_list);

      if(timeout==0) {
         list_cv.wait(lock, [&] {
         return lists[list_key].size()>0;
         });
         auto& dq = lists[list_key];
         std::string text = dq.front(); dq.pop_front();
         array.push_back(text);
      }
      else {
         ready = list_cv.wait_for(lock, std::chrono::milliseconds(timeout), [&] {
        return lists[list_key].size()>0;
      });
        if(ready) {
         auto& dq = lists[list_key];
         std::string text = dq.front(); dq.pop_front();
         array.push_back(text);
         ready=true;
      }
      }

      if(lists[list_key].size()==0) {
    lists.erase(list_key); 
    {
      std::unique_lock<std::shared_mutex> typelock(sm_keytype);
      key_type.erase(list_key);
    }
  }

    }

  if(!ready) return "*-1\r\n";



  return arr_to_resp(array);
}

std::string enum_to_string(KeyType t) {
    if (t == KeyType::string) return "string";
    if (t == KeyType::list) return "list";
    return "stream";
}

std::string TYPE(std::string key) {
  std::string response = "+none\r\n";
  {
    std::shared_lock<std::shared_mutex> lock(sm_keytype);
    if(key_type.find(key)!=key_type.end()) {
      response = "+"+enum_to_string(key_type[key])+"\r\n";
    }
  }
  return response;
}

std::string XADD(std::vector<std::string> command) {
  std::string stream_key = command[1];
  std::string stream_id = command[2];
  std::string stream_id_prev = "0-0";
  std::string response;

  if(streams.find(stream_key)!=streams.end()) {
    Stream& stream = streams[stream_key];
    auto last = std::prev(stream.entries.end());
    stream_id_prev = last->first;
  }

  std::string new_id = generate_stream_id(
    stream_id_prev,
    stream_id,
    current_time_ms()
);
  
  stream_id = new_id;
  
  bool compare = stream_id_compare(stream_id_prev, stream_id);

  if(stream_id=="0-0") return "-ERR The ID specified in XADD must be greater than 0-0\r\n";

  if(!compare) {
        response = "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
        return response;
  }
  
  StreamEntry stream_map;
  for(int i=3; i<command.size(); i+=2) {
      stream_map.fields[command[i]] = command[i+1];
    }

  {
    std::unique_lock<std::shared_mutex> lock(sm_stream);
    streams[stream_key].entries[stream_id] = stream_map;
    {
       std::unique_lock<std::shared_mutex> typelock(sm_keytype);
       if(key_type.find(stream_key)==key_type.end())
       {

         key_type[stream_key] = KeyType::stream;
       }
    }
  }

  stream_cv.notify_all();

  return bulk_string(stream_id);
}


std::string XRANGE(std::string stream_key, std::string start, std::string end) {
    std::string response;
 {
  std::shared_lock<std::shared_mutex> lock(sm_stream);
  auto sit = streams.find(stream_key);
  if(sit==streams.end()) return "*0\r\n";

  Stream& stream = sit->second;

  if (start == "-") start = "0-0";
  else if(start.find('-')==std::string::npos) start  += "-0";

  if (end == "+")
    end = "18446744073709551615-18446744073709551615";
  else if (end.find('-') == std::string::npos)
    end += "-18446744073709551615";

  auto it = stream.entries.lower_bound(start);

  int count = 0;

  while(it!=stream.entries.end() && stream_id_compare_equal(it->first, end) ) {
    count++;
    response += "*2\r\n";
    response += bulk_string(it->first);
    std::vector<std::string> array;
    for(auto& k : it->second.fields) {
      array.push_back(k.first);
      array.push_back(k.second);
    }
    
    response += arr_to_resp(array);
    ++it;
  }

  response = "*"+std::to_string(count)+"\r\n" + response;


 }

  return response;
}

std::string XREAD(std::vector<std::pair<std::string, std::string>> key_to_id, int64_t timeout) {
    std::shared_lock<std::shared_mutex> lock(sm_stream);

    auto check_any_data = [&]() -> bool {
        for (const auto& P : key_to_id) {
            const std::string& stream_key = P.first;
            const std::string& id = P.second;

            if (streams.find(stream_key) == streams.end()) continue;
            const Stream& stream = streams.at(stream_key);
            
            if (stream.entries.empty()) continue;
            auto it = stream.entries.upper_bound(id);
            if (it != stream.entries.end()) {
                return true;
            }
        }
        return false;
    };

    if (timeout > 0 && !check_any_data()) {
        bool detected = stream_cv.wait_for(lock, std::chrono::milliseconds(timeout), check_any_data);
        if (!detected) return "*-1\r\n"; 
    } 
    else if (timeout == 0 && !check_any_data()) {
        stream_cv.wait(lock, check_any_data);
    }

    if (!check_any_data()) {
        return "*-1\r\n";
    }

    int valid_streams = 0;
    for(const auto& P : key_to_id) {
        if (streams.find(P.first) == streams.end()) continue;
        auto it = streams[P.first].entries.upper_bound(P.second);
        if (it != streams[P.first].entries.end()) valid_streams++;
    }

    std::string responseFinal = "*" + std::to_string(valid_streams) + "\r\n";

    for (const auto& P : key_to_id) {
        std::string stream_key = P.first;
        std::string id = P.second;

        if (streams.find(stream_key) == streams.end()) continue;
        Stream& stream = streams[stream_key];

        auto it = stream.entries.upper_bound(id);
        if (it == stream.entries.end()) continue;

        std::string response;
        response += "*2\r\n$" + std::to_string(stream_key.size()) + "\r\n" + stream_key + "\r\n";
        
        int count = 0;
        std::string entries_resp;
        
        while (it != stream.entries.end()) {
            count++;
            entries_resp += "*2\r\n";
            entries_resp += bulk_string(it->first);
            std::vector<std::string> array;
            for (auto& k : it->second.fields) {
                array.push_back(k.first);
                array.push_back(k.second);
            }
            entries_resp += arr_to_resp(array);
            ++it;
        }
        
        response += "*" + std::to_string(count) + "\r\n" + entries_resp;
        responseFinal += response;
    }

    return responseFinal;
}


