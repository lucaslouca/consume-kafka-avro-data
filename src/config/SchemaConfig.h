/**
 * @file SchemaConfig
 *
 * @brief Plain-old-data structure to hold Avro schema configuration for serializer.
 *
 * @author Lucas Louca
 *
 */
#ifndef SCHEMA_CONFIG_H
#define SCHEMA_CONFIG_H

#include <string>
#include <vector>
#include <map>
#include <avro/Schema.hh>

struct SchemaConfig
{
    const std::string name;
    const std::string key_column;
    const std::vector<std::string> columns;
    const std::map<std::string, std::string> column_map;
    const std::map<std::string, std::string> column_type_transforms;
    avro::ValidSchema schema;
    int32_t schema_id;
};

#endif