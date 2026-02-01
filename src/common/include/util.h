//
// Created by 19327 on 2026/01/30/星期五.
//

#pragma once

#include <string>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>

void DPrintf(const char *format...);

void myAssert(bool condition, std::string message) {
    if (!condition) {
        std::cerr << "Error: " << message << std::endl;
        std::exit(EXIT_FAILURE);
    }
}

template <typename... Args>
std::string format(const char* format_str, Args... args) {
    int size_s = std::snprintf(nullptr, 0, format_str, args...) + 1;  // "\0"
    if (size_s <= 0) {
        throw std::runtime_error("Error during formatting.");
    }
    auto size = static_cast<size_t>(size_s);
    std::vector<char> buf(size);
    std::snprintf(buf.data(), size, format_str, args...);
    return std::string(buf.data(), buf.data() + size - 1);  // remove '\0'
}



template<class T>
class LockQueue {
private:
    std::mutex mutex;
    std::condition_variable cond;
    std::queue<T> queue;

public:
    void Push(const T &item) {
        std::lock_guard<std::mutex> lock(mutex);
        queue.push(item);
        cond.notify_all();
    }

    T Pop() {
        std::unique_lock<std::mutex> lock(mutex);
        while (queue.empty()) {
            // 这里用unique_lock是因为lock_guard不支持解锁，而unique_lock支持;
            cond.wait(lock);
        }
        T item = queue.front();
        queue.pop();
        return item;
    }

    bool timeOutPop(int timeOut, T *item) {
        std::unique_lock<std::mutex> lock(mutex);
        auto now = std::chrono::system_clock::now();
        auto timeOutTime = std::chrono::system_clock::now() + std::chrono::milliseconds(timeOut);
        while (queue.empty()) {
            if (cond.wait_until(lock, timeOutTime) == std::cv_status::timeout) {
                return false;
            } else {
                continue;
            }
        }
        T data = queue.front();
        queue.pop();
        *item = data;
        return true;
    }
};

class Op {
private:
    int64_t clientId;
    int64_t seqId;
    std::string command;
public:
    std::string toString() {
        return "clientId:" + std::to_string(clientId) + ", seqId:" + std::to_string(seqId) + ", command:" + command;
    }

    bool parseFromString(const std::string &str) {
        clientId = std::stoll(str.substr(0, str.find(",")));
        seqId = std::stoll(str.substr(str.find(",") + 1, str.find(",", str.find(",") + 1) - str.find(",") - 1));
        command = str.substr(str.find_last_of(",") + 1);
        return true;
    }
};

std::chrono::system_clock::time_point now() {
    return std::chrono::system_clock::now();
}

std::chrono::milliseconds getRandomizedElectionTimeout() {
    return std::chrono::milliseconds(rand() % 150 + 150);
};

void sleepForNMilliseconds(int N) {
    std::this_thread::sleep_for(std::chrono::milliseconds(N));
}

bool isReleasePort(unsigned short port) {
    int s = socket(AF_INET, SOCK_STREAM, IPPROTO_IP);
    sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);
    int ret = bind(s, (sockaddr *) &addr, sizeof(addr));
    if (ret != 0) {
        close(s);
        return false;
    }
    close(s);
    return true;
}
