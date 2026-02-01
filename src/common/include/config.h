//
// Created by 19327 on 2026/01/30/星期五.
//

#pragma once

const bool Debug = true;

const int debugMul = 1;

const int HeartBeatTimeOut = 25 * debugMul;
const int ApplyInterval = 10 * debugMul;

const int minRandElectionTimeOut = 300 * debugMul;
const int maxRandElectionTimeOut = 500 * debugMul;

const int consensusTimeOut = 500 * debugMul;

const int fiberThreadPoolSize = 2;
const bool fiberUseCallerThread = false;

const int64_t InvalidTerm=0;
const int64_t InvalidIndex=0;