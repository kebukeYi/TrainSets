//
// Created by 19327 on 2026/01/27/星期二.
//
#include "persisted.h"
#include "util.h"

Persisted::Persisted(int me) : snapshot_file_name_("snapshotPersist" + std::to_string(me) + ".txt"),
                               raft_state_file_name_("raftStatePersist" + std::to_string(me) + ".txt"),
                               raft_state_file_size_(0) {
    bool file_open_flag = true;
    std::fstream file(raft_state_file_name_, std::ios::out | std::ios::trunc);
    if (file.is_open()) {
        file.close();
    } else {
        file_open_flag = false;
    }
    if (!file_open_flag) {
        std::string error_message_;
        std::sprintf(&error_message_[0], "Failed to open file %s \n", raft_state_file_name_.c_str());
        DPrintf("%s", error_message_.c_str());
        return;
    }
    file = std::fstream(snapshot_file_name_, std::ios::out | std::ios::trunc);
    if (file.is_open()) {
        file.close();
    } else {
        file_open_flag = false;
    }
    if (!file_open_flag) {
        std::string error_message_;
        std::sprintf(&error_message_[0], "Failed to open file %s \n", snapshot_file_name_.c_str());
        DPrintf("%s", error_message_.c_str());
        return;
    }
    raft_state_file_out_.open(raft_state_file_name_);
    snapshot_file_out_.open(snapshot_file_name_);
}

Persisted::~Persisted() {
    if (raft_state_file_out_.is_open()) {
        raft_state_file_out_.close();
    }
    if (snapshot_file_out_.is_open()) {
        snapshot_file_out_.close();
    }
}

void Persisted::Save(std::string &raft_state, std::string &snapshot) {
    std::lock_guard<std::mutex> lock(lock_);
    clear_raft_state();
    clear_snapshot();
    raft_state_file_out_ << raft_state;
    snapshot_file_out_ << snapshot;
}

void Persisted::SaveRaftState(std::string &raft_state) {
    std::lock_guard<std::mutex> lock(lock_);
    clear_raft_state();
    raft_state_file_out_ << raft_state;
    raft_state_file_size_ += raft_state.size();
}

ssize_t Persisted::GetRaftStateFileSize() {
    std::lock_guard<std::mutex> lock(lock_);
    return raft_state_file_size_;
}

std::string Persisted::LoadRaftState() {
    std::lock_guard<std::mutex> lock(lock_);
    if (raft_state_file_out_.is_open()) {
        raft_state_file_out_.close();
    }
    std::ifstream rfi(raft_state_file_name_, std::ios::binary);
    if (!rfi.good()) {
        return "";
    }
    std::string raft_state;
    rfi.seekg(0, std::ios::end);
    raft_state.resize(rfi.tellg());
    rfi.seekg(0, std::ios::beg);
    rfi.read(&raft_state[0], raft_state.size());
    return raft_state;
}

std::string Persisted::LoadSnapshot() {
    std::lock_guard<std::mutex> lock(lock_);
    if (snapshot_file_out_.is_open()) {
        snapshot_file_out_.close();
    }
    std::ifstream rfi(snapshot_file_name_, std::ios::binary);
    if (!rfi.good()) {
        return "";
    }
    std::string snapshot;
    rfi.seekg(0, std::ios::end);
    snapshot.resize(rfi.tellg());
    rfi.seekg(0, std::ios::beg);
    rfi.read(&snapshot[0], snapshot.size());
    return snapshot;
}

void Persisted::clear_snapshot() {
    if (snapshot_file_out_.is_open()) {
        snapshot_file_out_.close();
    }
    snapshot_file_out_.open(snapshot_file_name_, std::ios::out | std::ios::trunc);
}

void Persisted::clear_raft_state() {
    raft_state_file_size_ = 0;
    if (raft_state_file_out_.is_open()) {
        raft_state_file_out_.close();
    }
    raft_state_file_out_.open(raft_state_file_name_, std::ios::out | std::ios::trunc);
}