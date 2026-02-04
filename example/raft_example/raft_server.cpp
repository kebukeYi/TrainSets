//
// Created by 19327 on 2026/02/02/星期一.
//
#include <csignal>
#include "raft.h"
#include "application_server.h"

void ShowArgsHelp();

int main(int argc, char **argv) {
    // ./example_raft_server -i 0 -n 3 -f ../conf/raft_node.conf -m ../conf/everysec.conf
    // ./example_raft_server -i 1 -n 3 -f ../conf/raft_node.conf -m ../conf/everysec.conf
    // ./example_raft_server -i 2 -n 3 -f ../conf/raft_node.conf -m ../conf/everysec.conf
    if (argc < 2) {
        ShowArgsHelp();
        exit(EXIT_FAILURE);
    }
    int nodeId = 0;
    int nodeSum = 0;
    std::string raftNodeConfigFilePath;
    std::string machineConfigFilePath;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "-i") {
            nodeId = std::stoi(argv[++i]);
        } else if (arg == "-n") {
            nodeSum = std::stoi(argv[++i]);
        } else if (arg == "-f") {
            raftNodeConfigFilePath = argv[++i];
        } else if (arg == "-m") {
            machineConfigFilePath = argv[++i];
        } else {
            std::cerr << "Invalid argument: " << arg << std::endl;
            ShowArgsHelp();
            exit(EXIT_FAILURE);
        }
    }

    ApplicationServer server(nodeId, 600, raftNodeConfigFilePath, machineConfigFilePath);


    // 控制起几个 raftNode;
//    for (int i = 0; i < nodeNum; i++) {
//        pid_t pid = fork();
//        if (pid == 0) {
//            ApplicationServer server(i, 600, raftNodeConfigFilePath, machineConfigFilePath);
//            pause();
//        } else if (pid > 0) {
//            sleep(1);
//        } else {
//            // 如果创建进程失败
//            std::cerr << "Failed to create child process." << std::endl;
//            exit(EXIT_FAILURE);
//        }
//    }
//    pause();
    return 0;
}

void ShowArgsHelp() { std::cout << "format: command -n <nodeNum> -f <configFileName>" << std::endl; }
