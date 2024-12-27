#include "Logging.h"
#include <spdlog/sinks/stdout_sinks.h>
#include <spdlog/sinks/daily_file_sink.h>

Logging::SpdLogger::SpdLogger(const Config &config) : BaseLogger(config)
{
    auto name = config.find("file_name");
    if (name == config.end())
        throw std::runtime_error("No output file provided to file logger");
    m_file_name = name->second;

    int hour = 2;
    int minute = 30;

    if (config.find("hour") != config.end())
    {
        hour = atoi(config.find("hour")->second.c_str());
    }

    if (config.find("minute") != config.end())
    {
        minute = atoi(config.find("minute")->second.c_str());
    }

    std::vector<spdlog::sink_ptr> sinks;
    sinks.push_back(std::make_shared<spdlog::sinks::stdout_sink_st>());
    // New file created every day at hour:minute am
    sinks.push_back(std::make_shared<spdlog::sinks::daily_file_sink_mt>(m_file_name, hour, minute));

    m_logger = std::make_shared<spdlog::logger>("logger", begin(sinks), end(sinks));
    m_logger->set_level(spdlog::level::trace);
    m_logger->sinks()[0]->set_pattern("%v");
    m_logger->sinks()[1]->set_pattern("%v");
    m_logger->flush_on(spdlog::level::trace);
    spdlog::register_logger(m_logger);
}

void Logging::SpdLogger::log(const std::string &message, const Level level, const std::string &name)
{
    if (level < LEVEL_CUTOFF)
    {
        return;
    }

    log(Logging::create_log(message, level, name));
}

void Logging::SpdLogger::log(const std::string &message, const Level level)
{
    if (level < LEVEL_CUTOFF)
    {
        return;
    }
    log(message);
}

void Logging::SpdLogger::log(const std::string &message)
{
    m_logger->trace(message);
}