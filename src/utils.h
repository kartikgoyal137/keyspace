#include <string>
#include <vector>

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
