//
// Created by 19327 on 2026/01/27/星期二.
//
#include "state_machine.h"

class StateMachineImpl : public StateMachine {
public:
    StateMachineImpl();

    ~StateMachineImpl();

    Result Cmd(const std::string &cmd) {
        return Result{};
    };
};