#include "Logging.h"

Logging::StdOutLogger::StdOutLogger(const Config &config) : BaseLogger(config) {}

void Logging::StdOutLogger::log(const std::string &message, const Logging::Level level, const std::string &name)
{
    if (level < LEVEL_CUTOFF)
    {
        return;
    }
    log(Logging::create_log(message, level, name));
}

void Logging::StdOutLogger::log(const std::string &message, const Level level)
{
    if (level < LEVEL_CUTOFF)
    {
        return;
    }
    log(message);
}

void Logging::StdOutLogger::log(const std::string &message)
{
    // cout is thread safe, to avoid multiple threads interleaving on one line
    // though, we make sure to only call the << operator once on std::cout
    // otherwise the << operators from different threads could interleave
    // obviously we dont care if flushes interleave
    // std::lock_guard<std::mutex> lk{lock};
    std::cout << message;
    std::cout.flush();
}