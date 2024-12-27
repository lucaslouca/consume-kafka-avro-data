#ifndef CONFIG_PARSER_H
#define CONFIG_PARSER_H

#include <yaml-cpp/yaml.h>

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "SchemaConfig.h"

class ConfigParser {
   private:
    ConfigParser(std::string c);
    std::string m_config_file;
    YAML::Node m_config;
    std::string key_column();  // {'type_map' : {'vendor_mobility_in': {'key_column': 'abc', 'columns': ['id_type',
                               // 'timestamp', ...]}}}
    std::map<std::string, std::string> config_for_key(const std::string &key);
    std::map<std::string, SchemaConfig> schema_configs();
    avro::ValidSchema assemble_schema(const SchemaConfig &config);
    avro::ValidSchema load_schema(const std::string file);
    int32_t fetch_schema_id_rest(const std::string &name, const std::string &registry);
    int32_t fetch_schema_id(const std::string &name);

   public:
    /*
    Deleted functions should generally
    be public as it results in better error messages
    due to the compilers behavior to check accessibility
    before deleted status
    */
    ConfigParser(const ConfigParser &) = delete;
    void operator=(const ConfigParser &) = delete;

    static ConfigParser &instance(std::string c);
    bool has_key(const std::string &k);
    std::map<std::string, std::string> kafka();
    std::map<std::string, std::string> column_map();
    std::map<std::string, std::string> column_type_transforms_map();
    std::map<std::string, SchemaConfig> schemas();
    ~ConfigParser();
};
#endif