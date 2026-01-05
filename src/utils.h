#include <string>
#include <vector>
#include <chrono>
#include <charconv>

long long current_time_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(
        system_clock::now().time_since_epoch()
    ).count();
}

int to_int(const std::string& s) {
    int value;
    auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), value);

    if (ec != std::errc() || ptr != s.data() + s.size()) {
      return -1;
    }
    return value;
}

bool arg_check(const std::string& a, const std::string& b) {
    if (a.size() != b.size()) return false;
    for (size_t i = 0; i < a.size(); ++i) {
        if (tolower(a[i]) != tolower(b[i])) return false;
    }
    return true;
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

std::string arr_to_resp(std::vector<std::string> array) {
  std::string response;
  int64_t size = array.size();  
  response = "*"+std::to_string(size)+"\r\n";
  for(auto& ele : array) {
    response += "$"+std::to_string(ele.size())+"\r\n"+ele+"\r\n";
  }

  return response;
}

std::string bulk_string(std::string text) {
  return "$"+std::to_string(text.length())+"\r\n"+text+"\r\n";
}

bool numStrLess(const std::string& a, const std::string& b) {
    if (a.size() != b.size())
        return a.size() < b.size();
    return a < b;
}

bool stream_id_compare(const std::string& id1, const std::string& id2) {
    auto p1 = id1.find('-');
    auto p2 = id2.find('-');

  std::string t1 = id1.substr(0, p1);
  std::string s1 = id1.substr(p1 + 1);

  std::string t2 = id2.substr(0, p2);
  std::string s2 = id2.substr(p2 + 1);

  if (t1 != t2)
        return numStrLess(t1, t2);

    return numStrLess(s1, s2);
}

bool stream_id_compare_equal(const std::string& a,
                             const std::string& b) {
    return stream_id_compare(a, b) || a == b;
}



std::string generate_stream_id(
    const std::string& last_id,
    const std::string& user_id,
    long long now_ms
) {
    long long last_t = 0, last_s = 0;

    if (last_id != "0-0") {
        auto p = last_id.find('-');
        last_t = std::stoll(last_id.substr(0, p));
        last_s = std::stoll(last_id.substr(p + 1));
    }

    if (user_id == "*") {
        if (now_ms > last_t)
            return std::to_string(now_ms) + "-0";
        else
            return std::to_string(last_t) + "-" + std::to_string(last_s + 1);
    }

    auto p = user_id.find('-');
    std::string t = user_id.substr(0, p);
    std::string s = user_id.substr(p + 1);

    if (s == "*") {
        long long T = std::stoll(t);
        if (T > last_t)
            return t + "-0";
        if (T == last_t)
            return t + "-" + std::to_string(last_s + 1);
    }

    return user_id;
}

