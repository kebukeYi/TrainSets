//
// Created by 19327 on 2026/01/27/星期二.
//

#pragma once

#include <string>

class ApplyMsg {
public:
    bool CommandValid;
    std::string Command;
    int64_t CommandIndex;

    bool SnapshotValid;
    std::string Snapshot;
    int64_t SnapshotIndex;
    int64_t SnapshotTerm;

    ApplyMsg() : CommandValid(false), Command(), CommandIndex(-1), SnapshotValid(false),
                 Snapshot(), SnapshotIndex(-1), SnapshotTerm(-1) {}

};