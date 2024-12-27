#include <_string.h>
#include <librdkafka/rdkafkacpp.h>
#include <signal.h>
#include <sys/event.h>  // for kqueue() etc.
#include <sys/stat.h>
#include <sys/types.h>

#include <cstring>
#include <future>  // for async()
#include <iostream>
#include <map>
#include <string>

#include "Database.h"
#include "KafkaConsumerCallback.h"
#include "KafkaPoller.h"
#include "SignalChannel.h"
#include "config/ConfigParser.h"
#include "logging/Logging.h"
static std::string name = "main";

/**
 * Create a return a shared channel for SIGINT signals.
 *
 */
std::shared_ptr<SignalChannel> listen_for_sigint(sigset_t &sigset) {
    std::shared_ptr<SignalChannel> sig_channel = std::make_shared<SignalChannel>();

#ifdef __linux__
    // Listen for sigint event line Ctrl^c
    sigemptyset(&sigset);
    sigaddset(&sigset, SIGINT);
    sigaddset(&sigset, SIGTERM);
    pthread_sigmask(SIG_BLOCK, &sigset, nullptr);

    std::thread signal_handler{[&sig_channel, &sigset]() {
        int signum = 0;

        // wait untl a signal is delivered
        sigwait(&sigset, &signum);
        sig_channel->m_shutdown_requested.store(true);

        // notify all waiting workers to check their predicate
        sig_channel->m_cv.notify_all();
        std::cout << "Received signal " << signum << "\n";
        return signum;
    }};
    signal_handler.detach();
#elif __APPLE__
    std::thread signal_handler{[&sig_channel]() {
        int kq = kqueue();

        /* Two kevent structs */
        struct kevent *ke = (struct kevent *)malloc(sizeof(struct kevent));

        /* Initialise struct for SIGINT */
        signal(SIGINT, SIG_IGN);
        EV_SET(ke, SIGINT, EVFILT_SIGNAL, EV_ADD, 0, 0, NULL);

        /* Register for the events */
        if (kevent(kq, ke, 1, NULL, 0, NULL) < 0) {
            perror("kevent");
            return false;
        }

        memset(ke, 0x00, sizeof(struct kevent));

        // Camp here for event
        if (kevent(kq, NULL, 0, ke, 1, NULL) < 0) {
            perror("kevent");
        }

        switch (ke->filter) {
            case EVFILT_SIGNAL:
                std::cout << "Received signal " << strsignal(ke->ident) << "\n";
                sig_channel->m_shutdown_requested.store(true);
                sig_channel->m_cv.notify_all();
                break;
            default:
                break;
        }

        return true;
    }};
    signal_handler.detach();
#endif

    return sig_channel;
}

/**
 * Print usage.
 *
 */
void usage(const std::string me) {
    std::cerr << "Usage: " << me
              << " [options]\n"
                 "Produces Avro encoded messages to Kafka from CSV objects\n"
                 "\n"
                 "Options:\n"
                 " -c <config>       Configuration file\n"
                 "\n"
                 "\n"
                 "Example:\n"
                 "  "
              << me << " -c lsm2kafka.yaml\n\n";
    exit(1);
}

/**
 * Parse commandline arguments and fill in config file path.
 *
 */
void parse_args(int argc, char *argv[], std::string &config_file) {
    int opt;
    while ((opt = getopt(argc, argv, "d:c:")) != -1) {
        switch (opt) {
            case 'c':
                config_file = optarg;
                break;
            default:
                std::cerr << "Unknown option -" << (char)opt << std::endl;
                usage(argv[0]);
        }
    }

    if (config_file.empty()) {
        std::cerr << "A config file must be provided" << std::endl;
        exit(1);
    } else {
        struct stat info;
        if (stat(config_file.c_str(), &info) != 0) {
            std::cerr << "Cannot access '" << config_file << "'" << std::endl;
            exit(1);
        } else if (info.st_mode & S_IFDIR) {
            std::cerr << "'" << config_file << "' is a directory" << std::endl;
            exit(1);
        }
    }
}

// Server side
int main(int argc, char *argv[]) {
    std::string config_file;

    /*************************************************************************
     *
     * COMMANDLINE ARGUMENTS
     *
     *************************************************************************/
    parse_args(argc, argv, config_file);

    /*************************************************************************
     *
      /*************************************************************************
       *
       * SIGINT CHANNEL
       *
       *************************************************************************/
    sigset_t sigset;
    std::shared_ptr<SignalChannel> sig_channel = listen_for_sigint(sigset);

    /*************************************************************************
     *
     * LOGGER
     *
     *************************************************************************/
    std::atomic<size_t> active_processors = 0;
    std::condition_variable log_cv;
    std::mutex log_cv_mutex;
    Logging::LogProcessor log_processor(&active_processors, &log_cv, &log_cv_mutex);

    log_processor.start();

    /*************************************************************************
     *
     * CONFIGURATION
     *
     *************************************************************************/
    ConfigParser &config = ConfigParser::instance(config_file);

    /*************************************************************************
     *
     * DATABASE
     *
     *************************************************************************/
    Logging::INFO("Connecting to database...", name);
    Database::init("hostaddr=127.0.0.1 port=5432 dbname=odynet user=postgres password=example");
    Logging::INFO("Connected to database", name);

    /*************************************************************************
     *
     * KAFKA
     *
     *************************************************************************/
    std::map<std::string, SchemaConfig> schemas = config.schemas();
    std::map<std::string, std::string> kafka_config = config.kafka();

    RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    std::string errstr;
    if (conf->set("bootstrap.servers", kafka_config["bootstrap.servers"], errstr) != RdKafka::Conf::CONF_OK) {
        Logging::ERROR(errstr, name);
        kill(getpid(), SIGINT);
    }
    if (conf->set("client.id", kafka_config["client.id"], errstr) != RdKafka::Conf::CONF_OK) {
        Logging::ERROR(errstr, name);
        kill(getpid(), SIGINT);
    }

    conf->set("enable.partition.eof", "true", errstr);

    // Create a consumer handle
    RdKafka::Consumer *consumer = RdKafka::Consumer::create(conf, errstr);
    if (!consumer) {
        Logging::ERROR("Failed to create consumer: " + errstr, name);
        exit(1);
    }
    Logging::INFO("Created consumer " + consumer->name(), name);

    RdKafka::Conf *tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
    std::string topic_str = "spo";
    RdKafka::Topic *topic = RdKafka::Topic::create(consumer, topic_str, tconf, errstr);
    if (!topic) {
        Logging::ERROR("Failed to create topic: " + errstr, name);
        exit(1);
    }

    Logging::INFO("Starting the consumer handle", name);
    int64_t start_offset = RdKafka::Topic::OFFSET_BEGINNING;  // RdKafka::Topic::OFFSET_STORED
    int32_t partition = 0;
    RdKafka::ErrorCode resp = consumer->start(topic, partition, start_offset);
    if (resp != RdKafka::ERR_NO_ERROR) {
        Logging::ERROR("Failed to start consumer: " + RdKafka::err2str(resp), name);
        exit(1);
    }

    KafkaConsumerCallback consumer_cb;
    /*
     * Consume messages
     * https://github.com/confluentinc/librdkafka/blob/master/examples/rdkafka_example.cpp
     */
    int use_ccb = 0;
    size_t errors = 0;
    while (1) {
        if (use_ccb) {
            consumer->consume_callback(topic, partition, 1000, &consumer_cb, &use_ccb);
        } else {
            RdKafka::Message *msg = consumer->consume(topic, partition, 1000);
            if (!consumer_cb.consume_message(msg)) {
                ++errors;
                Logging::ERROR("Number of failed deserializations: " + std::to_string(errors), name);
            }
            delete msg;
        }
        consumer->poll(0);
    }

    /*
     * Stop consumer
     */
    consumer->stop(topic, partition);

    consumer->poll(1000);

    delete topic;
    delete consumer;

    return 0;
}