/**
 * Courtesy of Kevin Kreiser
 *
 **/
#ifndef LOGGING_H
#define LOGGING_H
#define LOGGING_LEVEL_INFO

#include <spdlog/spdlog.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>

#include "SafeQueue.h"

extern SafeQueue<std::string> log_queue;

namespace Logging {
enum class Level : uint8_t { TRACE = 0, DEBUG = 1, INFO = 2, WARN = 3, ERROR = 4 };

struct level_hashing_function {
    template <typename T>
    std::size_t operator()(T t) const {
        return static_cast<std::size_t>(t);
    }
};

const std::unordered_map<Level, std::string, level_hashing_function> prefix{{Level::ERROR, " [ERROR] "},
                                                                            {Level::WARN, " [WARN] "},
                                                                            {Level::INFO, " [INFO] "},
                                                                            {Level::DEBUG, " [DEBUG] "},
                                                                            {Level::TRACE, " [TRACE] "}};

#if defined(LOGGING_LEVEL_ALL) || defined(LOGGING_LEVEL_TRACE)
constexpr Level LEVEL_CUTOFF = Level::TRACE;
#elif defined(LOGGING_LEVEL_DEBUG)
constexpr Level LEVEL_CUTOFF = Level::DEBUG;
#elif defined(LOGGING_LEVEL_WARN)
constexpr Level LEVEL_CUTOFF = Level::WARN;
#elif defined(LOGGING_LEVEL_ERROR)
constexpr Level LEVEL_CUTOFF = Level::ERROR;
#elif defined(LOGGING_LEVEL_NONE)
constexpr Level LEVEL_CUTOFF = Level::ERROR + 1;
#else
constexpr Level LEVEL_CUTOFF = Level::INFO;
#endif

/**
    Timestamp as: year/mo/dy hr:mn:sc.xxxxxx
*/
inline std::string timestamp() {
    std::chrono::system_clock::time_point tp =
        std::chrono::system_clock::now();  // Implementation dependant: microseconds or nanoseconds.
    std::time_t tt = std::chrono::system_clock::to_time_t(tp);  // Seconds since the Epoch
    std::tm gmt{};
    gmtime_r(&tt, &gmt);

    // from_time_t(...) returns "rounded" value
    // 1 microsecond = 0.000001 seconds
    // 1 nanosecond = 0.000000001 seconds or 1000 000 000ns = 1s
    // (1 668 496 399 029 524 297ns - 1 668 496 399 000 000 000ns) + gmt.tm_sec = 29 524 297ns + 19s = 19.029s
    std::chrono::duration<double> fractional_seconds =
        (tp - std::chrono::system_clock::from_time_t(tt)) + std::chrono::seconds(gmt.tm_sec);

    std::string buffer("year/mo/dy hr:mn:sc.xxxxxx");
    sprintf(&buffer.front(), "%04d/%02d/%02d %02d:%02d:%09.6f", gmt.tm_year + 1900, gmt.tm_mon + 1, gmt.tm_mday,
            gmt.tm_hour, gmt.tm_min, fractional_seconds.count());
    return buffer;
}

using Config = std::unordered_map<std::string, std::string>;

/**
 *  Base Logger
 **/
class BaseLogger {
   protected:
    std::mutex m_lock;

   public:
    BaseLogger() = delete;
    BaseLogger(const Config &) {};
    virtual ~BaseLogger() {};
    virtual void log(const std::string &, const Level, const std::string &name) {};
    virtual void log(const std::string &message, const Level level) {};
    virtual void log(const std::string &) {};
};

/**
 *  Standard Output Logger
 **/
class StdOutLogger : public BaseLogger {
   public:
    StdOutLogger() = delete;
    StdOutLogger(const Config &config);
    virtual void log(const std::string &message, const Level level, const std::string &name) override;
    virtual void log(const std::string &message, const Level level) override;
    virtual void log(const std::string &message) override;
};

/**
 *  File Logger
 **/
class FileLogger : public BaseLogger {
   public:
    FileLogger() = delete;
    FileLogger(const Config &config);
    virtual void log(const std::string &message, const Level level, const std::string &name);
    virtual void log(const std::string &message, const Level level) override;
    virtual void log(const std::string &message);

   protected:
    void reopen();
    std::string m_file_name;
    std::ofstream m_file;
    std::chrono::seconds m_reopen_interval;
    std::chrono::system_clock::time_point m_last_reopen;
};

/**
 *  File Logger using the spdlog lib
 **/
class SpdLogger : public BaseLogger {
   public:
    SpdLogger() = delete;
    SpdLogger(const Config &config);
    virtual void log(const std::string &message, const Level level, const std::string &name);
    virtual void log(const std::string &message, const Level level) override;
    virtual void log(const std::string &message);

   protected:
    std::string m_file_name;

   private:
    std::shared_ptr<spdlog::logger> m_logger;
};
/**
 *  Factory
 **/
class Factory {
    using logger_creator = std::unique_ptr<BaseLogger> (*)(const Config &);

   protected:
    std::unordered_map<std::string, logger_creator> m_creators;

   public:
    Factory();
    std::unique_ptr<Logging::BaseLogger> create(const Config &config) const;
};

// statically get a factory
inline Factory &factory() {
    static Factory factory_singleton{};
    return factory_singleton;
}

inline BaseLogger &get_logger(const Config &config = {{"type", "std_out"}}) {
    static std::unique_ptr<BaseLogger> singleton(factory().create(config));
    return *singleton;
}

// configure the singleton (once only)
inline void configure(const Config &config) { get_logger(config); }

inline std::string create_log(const std::string &message, const Level level, const std::string name) {
    std::string output;

    std::size_t len = name.length() + message.length() + 64;
    output.reserve(len);
    output.append(timestamp());
    output.append(prefix.find(level)->second);
    output.append("[");
    output.append(name);
    output.append("] ");
    output.append(message);
    return output;
}

// statically log
inline void log(const std::string &message, const Level level, const std::string &name) {
    get_logger().log(message, level, name);
}

inline void log(const std::string &message, const Level level) { get_logger().log(message, level); }

// statically log manually without a level
inline void log(const std::string &message) { get_logger().log(message); }

inline void TRACE(const std::string &message, const std::string &name = "") {
    if (LEVEL_CUTOFF > Level::TRACE) {
        return;
    }

    log_queue.enqueue(create_log(message, Level::TRACE, name));
}

inline void DEBUG(const std::string &message, const std::string &name = "") {
    if (LEVEL_CUTOFF > Level::DEBUG) {
        return;
    }

    log_queue.enqueue(create_log(message, Level::DEBUG, name));
}

inline void INFO(const std::string &message, const std::string &name = "") {
    // get_logger().log(message, Level::INFO);
    if (LEVEL_CUTOFF > Level::INFO) {
        return;
    }

    log_queue.enqueue(create_log(message, Level::INFO, name));
}

inline void WARN(const std::string &message, const std::string &name = "") {
    if (LEVEL_CUTOFF > Level::WARN) {
        return;
    }

    log_queue.enqueue(create_log(message, Level::WARN, name));
}

// TODO: Give LogProccessor priority on ERROR so that we don't miss any errors
inline void ERROR(const std::string &message, const std::string &name = "") {
    log_queue.enqueue(create_log(message, Level::ERROR, name));
}

/**
 * Thread that picks up log events from the queue and actually logs them.
 *
 **/
class LogProcessor {
   private:
    bool m_should_run;
    std::unique_ptr<std::thread> m_t;
    std::atomic<size_t> *m_active_processors;
    std::condition_variable *m_log_cv;
    std::mutex *m_log_cv_mutex;
    void run();

   public:
    LogProcessor(std::atomic<size_t> *active_processors, std::condition_variable *log_cv, std::mutex *log_cv_mutex);
    ~LogProcessor();
    bool start();
    void join() const;
    void stop();
};

}  // namespace Logging
#endif