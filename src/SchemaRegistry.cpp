#include "SchemaRegistry.h"

#include <signal.h>

#include <iostream>
#include <sstream>
#include <thread>

#include "logging/Logging.h"

static const std::string name = "SchemaRegistry";

SchemaRegistry::SchemaRegistry(const std::string *h) {
    if (h) {
        Serdes::Conf *m_sconf = Serdes::Conf::create();

        std::string errstr;
        if (m_sconf->set("schema.registry.url", *h, errstr)) {
            Logging::ERROR("Configuration failed: " + errstr, name);
            kill(getpid(), SIGINT);
        }

        /* Default framing CP1 */
        if (m_sconf->set("serializer.framing", "cp1", errstr)) {
            Logging::ERROR("Configuration failed: " + errstr, name);
            kill(getpid(), SIGINT);
        }

        m_serdes = Serdes::Avro::create(m_sconf, errstr);
        if (!m_serdes) {
            Logging::ERROR("Failed to create Serdes handle: " + errstr, name);
            kill(getpid(), SIGINT);
        }

        m_uninitialized.store(false);
    }
}

SchemaRegistry &SchemaRegistry::instance_impl(const std::string *h = nullptr) {
    static SchemaRegistry i{h};
    return i;
}

void SchemaRegistry::init(const std::string h) { instance_impl(&h); }

SchemaRegistry &SchemaRegistry::instance() {
    SchemaRegistry &i = instance_impl();

    if (i.m_uninitialized.load()) {
        throw std::logic_error("SchemaRegistry was not initialized");
    }

    return i;
}

int SchemaRegistry::fetch_value_schema_id(const std::string &schema_name) {
    if (!schema_name.empty()) {
        std::string errstr;
        Serdes::Schema *schema = Serdes::Schema::get(m_serdes, schema_name + "-value", errstr);
        if (schema) {
            std::stringstream ss;
            ss << "Fetched schema: '" << schema->name() << "'"
               << ", id: " << schema->id();

            Logging::INFO(ss.str(), name);

            ss.str("");
            avro::ValidSchema *avro_schema = schema->object();
            avro_schema->toJson(ss);
            Logging::INFO(ss.str(), name);

            return schema->id();
        } else {
            std::stringstream ss;
            ss << "No schema with name '" << schema_name << "'"
               << " found";
            Logging::INFO(ss.str(), name);
        }
    }

    return -1;
}

Serdes::Schema *SchemaRegistry::fetch_value_schema(const std::string &schema_name) {
    if (!schema_name.empty()) {
        std::string errstr;
        Serdes::Schema *schema = Serdes::Schema::get(m_serdes, schema_name + "-value", errstr);
        if (schema) {
            std::stringstream ss;
            ss << "Fetched schema: '" << schema->name() << "'"
               << ", id: " << schema->id();

            // Logging::INFO(ss.str(), name);
            return schema;
        } else {
            std::stringstream ss;
            ss << "No schema with name '" << schema_name << "'"
               << " found";
            Logging::INFO(ss.str(), name);
        }
    }

    return nullptr;
}

// int SchemaRegistry::avro2json(Serdes::Schema *schema, const avro::GenericDatum *datum, std::string &str,
//                               std::string &errstr) {
//     avro::ValidSchema *avro_schema = schema->object();

//     /* JSON encoder */
//     avro::EncoderPtr json_encoder = avro::jsonEncoder(*avro_schema);

//     /* JSON output stream */
//     std::ostringstream oss;
//     auto json_os = avro::ostreamOutputStream(oss);

//     try {
//         /* Encode Avro datum to JSON */
//         json_encoder->init(*json_os.get());
//         avro::encode(*json_encoder, *datum);
//         json_encoder->flush();

//     } catch (const avro::Exception &e) {
//         errstr = std::string("Binary to JSON transformation failed: ") + e.what();
//         return -1;
//     }

//     str = oss.str();
//     return 0;
// }

// int SchemaRegistry::deserialize(RdKafka::Message *message) {
//     std::string out;
//     std::string errstr;
//     avro::GenericDatum *d = NULL;
//     Serdes::Schema *schema = SchemaRegistry::instance().fetch_value_schema("spo");
//     if (m_serdes->deserialize(&schema, &d, message->payload(), message->len(), errstr) == -1 ||
//         avro2json(schema, d, out, errstr) == -1) {
//         std::cout << "Error: " << errstr << std::endl;
//     }
//     std::cout << "JSON: " << out << std::endl;
// }

int SchemaRegistry::register_value_schema(const std::string &schema_name, const std::string &schema_def) {
    std::string errstr;
    Serdes::Schema *schema = Serdes::Schema::add(m_serdes, schema_name + "-value", schema_def, errstr);

    if (schema) {
        std::stringstream ss;
        ss << "Registered schema: '" << schema->name() << "'"
           << ", id: " << schema->id();
        Logging::INFO(ss.str(), name);
        return schema->id();
    } else {
        std::stringstream ss;
        ss << "Failed to register new schema: '" << schema_name << "'"
           << ", id: " << schema->id() << ", definition: " << schema_def;
        Logging::ERROR(ss.str(), name);
    }
    return -1;
}