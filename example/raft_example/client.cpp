//
// Created by 19327 on 2026/02/02/星期一.
//
#include "client.h"
#include "util.h"

void ShowArgsHelp() { std::cout << "format: command -f <configFileName>" << std::endl; }

int main(int argc, char **argv) {
    // ./example_raft_client -f ../conf/raft_node.conf
    Client client;
    if (argc < 2) {
        ShowArgsHelp();
        exit(EXIT_FAILURE);
    }
    std::string raftNodeConfigFilePath;
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "-f") {
            raftNodeConfigFilePath = argv[++i];
        } else {
            std::cerr << "Invalid argument: " << arg << std::endl;
            ShowArgsHelp();
            exit(EXIT_FAILURE);
        }
    }

    client.init(raftNodeConfigFilePath);
    auto start = now();
    int count = 5;
    int tmp = count;
    while (tmp--) {
        client.set("key" + std::to_string(tmp), "value" + std::to_string(tmp));
        std::string get1 = client.get("key" + std::to_string(tmp));
        if (get1.empty() || get1 == "") {
            std::printf("get(%s) return empty\n", ("key" + std::to_string(tmp)).c_str());
        } else {
            std::printf("get(%s) = %s\n", ("key" + std::to_string(tmp)).c_str(), get1.c_str());
        }
    }
}