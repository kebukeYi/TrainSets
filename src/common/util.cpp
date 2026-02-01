//
// Created by 19327 on 2026/01/30/星期五.
//
#include <ctime>
#include <cstdio>
#include <cstdarg>

#include "config.h"
#include "util.h"

void DPrintf(const char *format, ...) {
    if (Debug) {
        time_t now = time(0);
        tm *ltm = localtime(&now);
        va_list args;
        va_start(args, format);
        std::printf("[%d-%d-%d-%d-%d-%d]",
                    ltm->tm_year + 1900, ltm->tm_mon + 1, ltm->tm_mday,
                    ltm->tm_hour, ltm->tm_min, ltm->tm_sec);
        std::vprintf(format, args);
        std::printf("\n");
        va_end(args);
    }
}

