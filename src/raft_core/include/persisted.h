//
// Created by 19327 on 2026/01/27/星期二.
//
#pragma once
#include <string>
#include <mutex>
#include <fstream>
#include <cstdint>

class Persisted {
public:
    std::mutex lock_;
    std::string raft_state_; // term log index
    std::string snapshot_;// snapshot

    const std::string raft_state_file_name_;
    const std::string snapshot_file_name_;

    std::ofstream raft_state_file_out_;
    std::ofstream snapshot_file_out_;
    //
    ssize_t raft_state_file_size_ = 0;

public:
    explicit Persisted(const int node_id);

    ~Persisted();

    void Save(std::string &raft_state, std::string &snapshot);

    void SaveRaftState(std::string &raft_state);

    std::string LoadRaftState();

    std::string LoadSnapshot();

    ssize_t GetRaftStateFileSize();

private:
    void clear_raft_state();

    void clear_snapshot();
};