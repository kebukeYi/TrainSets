//
// Created by 19327 on 2026/02/02/星期一.
//
#include "client.h"
#include "util.h"

int main(int args, char **argv) {
    Client client;
    RespParser respParser;
    std::string conf_file_name("raft_node.conf");
    client.init(conf_file_name);
    auto start = now();
    int count = 5;
    int tmp = count;
    while (tmp--) {
        client.set("key" + std::to_string(tmp), "value" + std::to_string(tmp));
        std::string get1 = client.get("key" + std::to_string(tmp));
        if (get1.empty()) {
            std::printf("get(%s) return empty\r\n", ("key" + std::to_string(tmp)).c_str());
        } else {
            respParser.append(get1);
            while (respParser.tryParseOne()) {
                auto respValue = respParser.tryParseOne();
                if (respValue.has_value()) {
                    auto value = respValue.value();
                    std::string bulk;
                    auto res = client.RespValueToString(value, bulk);
                    std::printf("get return :{%s}\r\n", res.c_str());
                }
            }
            respParser.clear();
        }
    }
}