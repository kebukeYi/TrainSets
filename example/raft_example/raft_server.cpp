//
// Created by 19327 on 2026/02/02/星期一.
//
#include "raft.h"
#include "application_server.h"

void ShowArgsHelp();

int main(int argc, char **argv) {
    // -i 1 -n 3 -f raft_node.conf
    if (argc < 2) {
        ShowArgsHelp();
        exit(EXIT_FAILURE);
    }
    int nodeNum = 0;
    int me =-1;
    std::string configFileName;
    char c;
    while ((c = getopt(argc, argv, "i:n:f:")) != -1) {
        switch (c) {
            case 'i':
                me = atoi(optarg);
                break;
            case 'n':
                nodeNum = atoi(optarg);
                break;
            case 'f':
                configFileName = optarg;
                break;
            default:
                ShowArgsHelp();
                exit(EXIT_FAILURE);
        }
    }

//    std::string machineFilePath = "machine_" + std::to_string(me) + ".txt";
//    ApplicationServer server(me, 600, configFileName, machineFilePath);

    for (int i = 0; i < nodeNum; i++) {
        std::string machineFilePath = "machine_" + std::to_string(i) + ".txt";
        pid_t pid = fork();
        if (pid == 0) {
            ApplicationServer server(i, 600, configFileName, machineFilePath);
            pause();
        } else if (pid > 0) {
            sleep(1);
        } else {
            // 如果创建进程失败
            std::cerr << "Failed to create child process." << std::endl;
            exit(EXIT_FAILURE);
        }
    }
    pause();
    return 0;
}

void ShowArgsHelp() { std::cout << "format: command -n <nodeNum> -f <configFileName>" << std::endl; }
