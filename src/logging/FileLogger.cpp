#include "Logging.h"

Logging::FileLogger::FileLogger(const Config &config) : BaseLogger(config)
{
    auto name = config.find("file_name");
    if (name == config.end())
        throw std::runtime_error("No output file provided to file logger");
    m_file_name = name->second;

    // if we specify an interval
    m_reopen_interval = std::chrono::seconds(300);
    auto interval = config.find("reopen_interval");
    if (interval != config.end())
    {
        try
        {
            m_reopen_interval = std::chrono::seconds(std::stoul(interval->second));
        }
        catch (...)
        {
            throw std::runtime_error(interval->second + " is not a valid reopen interval");
        }
    }
    reopen();
}

void Logging::FileLogger::log(const std::string &message, const Level level, const std::string &name)
{
    if (level < LEVEL_CUTOFF)
    {
        return;
    }

    log(Logging::create_log(message, level, name));
}

void Logging::FileLogger::log(const std::string &message, const Level level)
{
    if (level < LEVEL_CUTOFF)
    {
        return;
    }
    log(message);
}

void Logging::FileLogger::log(const std::string &message)
{
    m_lock.lock();
    m_file << message;
    m_file.flush();
    m_lock.unlock();
    reopen();
}

void Logging::FileLogger::reopen()
{
    // Periodically close and repone the file handle to make sure the contents of the file buffer
    // are written to disk.
    // check if it should be closed and reopened
    auto now = std::chrono::system_clock::now();
    m_lock.lock();
    if (now - m_last_reopen > m_reopen_interval)
    {
        m_last_reopen = now;
        try
        {
            m_file.close();
        }
        catch (...)
        {
        }
        try
        {
            m_file.open(m_file_name, std::ofstream::out | std::ofstream::app);
            m_last_reopen = std::chrono::system_clock::now();
        }
        catch (std::exception &e)
        {
            try
            {
                m_file.close();
            }
            catch (...)
            {
            }
            throw e;
        }
    }
    m_lock.unlock();
}