//
// Created by 19327 on 2026/01/27/星期二.
//
#pragma once
#include <string>

class StateMachine {
private:

public:
    struct Result {
        int Code;
        int64_t Index;
        std::string Value;
        std::string Error;
    };

    StateMachine();

    ~StateMachine() = default;

    Result Cmd(const std::string &cmd);
};
