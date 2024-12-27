#ifndef SCHEMA_REGISTRY_H
#define SCHEMA_REGISTRY_H

#include <librdkafka/rdkafkacpp.h>
#include <libserdes/serdescpp-avro.h>
#include <libserdes/serdescpp.h>

#include <atomic>
#include <avro/Compiler.hh>
#include <avro/Decoder.hh>
#include <avro/Encoder.hh>
#include <avro/Generic.hh>
#include <avro/Schema.hh>
#include <avro/Specific.hh>
#include <avro/ValidSchema.hh>
#include <mutex>
#include <string>

class SchemaRegistry {
   private:
    std::atomic<bool> m_uninitialized;
    SchemaRegistry(const std::string *h);
    static SchemaRegistry &instance_impl(const std::string *h);

   public:
    Serdes::Avro *m_serdes;
    SchemaRegistry(const SchemaRegistry &) = delete;
    void operator=(const SchemaRegistry &) = delete;

    static void init(const std::string h);
    static SchemaRegistry &instance();
    // int deserialize(RdKafka::Message *message);
    // int avro2json(Serdes::Schema *schema, const avro::GenericDatum *datum, std::string &str, std::string &errstr);
    int fetch_value_schema_id(const std::string &schema_name);
    Serdes::Schema *fetch_value_schema(const std::string &schema_name);
    int register_value_schema(const std::string &schema_name, const std::string &schema_def);
};

#endif