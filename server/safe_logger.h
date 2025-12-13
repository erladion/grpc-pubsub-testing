#ifndef SAFE_LOGGER_H
#define SAFE_LOGGER_H

#include <iostream>
#include <mutex>
#include <string>

class Logger {
public:
  enum Type { Info, Warning, Error, Fatal };

  static void Log(Type type, const std::string& msg) {
    static std::mutex logMutex;  // Locked for the duration of the print
    std::lock_guard<std::mutex> lock(logMutex);

    std::cout << "[" << typeToStr(type) << "]" << msg << std::endl;
  }

private:
  static std::string typeToStr(Type type) {
    std::string typeStr;
    switch (type) {
      case Info:
        typeStr = "INFO";
        break;
      case Warning:
        typeStr = "WARNING";
        break;
      case Error:
        typeStr = "ERROR";
        break;
      case Fatal:
        typeStr = "FATAL";
        break;
    }
    return typeStr;
  }
};

#endif
