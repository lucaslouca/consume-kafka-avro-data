#include "KafkaConsumerCallback.h"

#include <cpprest/http_client.h>  // web::json::value

#include <ctime>
#include <iomanip>
#include <sstream>

#include "Database.h"
#include "logging/Logging.h"
KafkaConsumerCallback::KafkaConsumerCallback() { m_schema = SchemaRegistry::instance().fetch_value_schema("spo"); }

bool KafkaConsumerCallback::consume_message(RdKafka::Message *message) {
    bool exit_eof = false;
    switch (message->err()) {
        case RdKafka::ERR__TIMED_OUT:
            break;
        case RdKafka::ERR_NO_ERROR:
            return deserialize(message) > 0;
            break;
        case RdKafka::ERR__PARTITION_EOF:
            /* Last message */
            if (exit_eof) {
                return false;
            }
            break;
        case RdKafka::ERR__UNKNOWN_TOPIC:
        case RdKafka::ERR__UNKNOWN_PARTITION:
            Logging::ERROR("Consume failed: " + message->errstr(), m_name);
            return false;
            break;
        default:
            /* Errors */
            Logging::ERROR("Consume failed: " + message->errstr(), m_name);
            return false;
    }
    return true;
}

void KafkaConsumerCallback::consume_cb(RdKafka::Message &msg, void *opaque) { consume_message(&msg); }

int KafkaConsumerCallback::avro2json(Serdes::Schema *schema, const avro::GenericDatum *datum, std::string &str,
                                     std::string &errstr) {
    avro::ValidSchema *avro_schema = schema->object();

    /* JSON encoder */
    avro::EncoderPtr json_encoder = avro::jsonEncoder(*avro_schema);

    /* JSON output stream */
    std::ostringstream oss;
    auto json_os = avro::ostreamOutputStream(oss);

    try {
        /* Encode Avro datum to JSON */
        json_encoder->init(*json_os.get());
        avro::encode(*json_encoder, *datum);
        json_encoder->flush();

    } catch (const avro::Exception &e) {
        errstr = std::string("Binary to JSON transformation failed: ") + e.what();
        Logging::ERROR(errstr, m_name);
        return -1;
    }

    str = oss.str();
    return 0;
}

size_t KafkaConsumerCallback::deserialize(RdKafka::Message *message) {
    std::string errstr;

    // https://github.com/confluentinc/libserdes/blob/master/examples/kafka-serdes-avro-console-consumer.cpp

    std::string out;
    avro::GenericDatum *d = NULL;

    if (SchemaRegistry::instance().m_serdes->deserialize(&m_schema, &d, message->payload(), message->len(), errstr) ==
            -1 ||
        avro2json(m_schema, d, out, errstr) == -1) {
        Logging::ERROR("Failed to deserialize: " + errstr, m_name);
    }
    if (!out.empty()) {
        web::json::value json_value = web::json::value::parse(out);
        std::string subject = json_value["subject"].as_string();
        std::string predicate = json_value["predicate"].as_string();
        std::string object = json_value["object"].as_string();

        auto t = std::time(nullptr);
        auto tm = *std::localtime(&t);

        std::ostringstream oss;
        oss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");
        std::string created_at = oss.str();

        if (Database::instance().insert_object(subject, "MyObjectType", created_at) &&
            Database::instance().insert_object(object, "MyObjectType", created_at)) {
            int source_id = Database::instance().get_object_id(subject);
            int target_id = Database::instance().get_object_id(object);
            if (!Database::instance().insert_relationship(source_id, target_id, predicate)) {
                Logging::ERROR("Could not persist predicate", m_name);
            }
        } else {
            Logging::ERROR("Could not persist either subject or object", m_name);
        }
    }

    delete d;
    return out.length();
}

KafkaConsumerCallback::~KafkaConsumerCallback() { delete m_schema; }