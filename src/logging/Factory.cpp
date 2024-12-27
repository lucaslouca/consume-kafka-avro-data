#include "Logging.h"
#include <sstream>

Logging::Factory::Factory()
{
    m_creators.emplace("", [](const Config &config) -> std::unique_ptr<BaseLogger>
                       { return std::make_unique<BaseLogger>(config); });
    m_creators.emplace("std_out", [](const Config &config) -> std::unique_ptr<BaseLogger>
                       { return std::make_unique<StdOutLogger>(config); });
    m_creators.emplace("file", [](const Config &config) -> std::unique_ptr<BaseLogger>
                       { return std::make_unique<FileLogger>(config); });
    m_creators.emplace("daily", [](const Config &config) -> std::unique_ptr<BaseLogger>
                       { return std::make_unique<SpdLogger>(config); });
}

std::unique_ptr<Logging::BaseLogger> Logging::Factory::create(const Config &config) const
{
    // grab the type
    auto type = config.find("type");
    if (type == config.end())
    {
        throw std::runtime_error("Logging factory configuration requires a type of logger.");
    }

    // grab the logger
    auto found = m_creators.find(type->second);
    if (found != m_creators.end())
    {
        return found->second(config);
    }

    // couldn't get a logger
    std::stringstream ss;
    std::string sep = "";
    for (const auto &[type, fct] : m_creators)
    {
        ss << sep << type;
        sep.assign(", ");
    }
    throw std::runtime_error("Couldn't produce logger for type: '" + type->second + "'. Valid type are: " + ss.str());
}