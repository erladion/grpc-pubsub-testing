#ifndef SAFE_LOGGER_H
#define SAFE_LOGGER_H

#include <iostream>
#include <mutex>
#include <string>

class Logger {
public:
  static void Log(const std::string& msg) {
    static std::mutex logMutex;  // Locked for the duration of the print
    std::lock_guard<std::mutex> lock(logMutex);
    std::cout << msg << std::endl;
  }

  static void LogError(const std::string& msg) {
    static std::mutex logMutex;
    std::lock_guard<std::mutex> lock(logMutex);
    std::cerr << "[ERROR] " << msg << std::endl;
  }
};

#endif
