#pragma once

#include "duckdb.hpp"

namespace duckdb {

class ExtensionLoader; // Forward declaration

class NanodbcExtension {
public:
    static void Load(ExtensionLoader &loader);
    
    static std::string Name() {
        return "nanodbc";
    }
    
    static std::string Version() {
#ifdef EXT_VERSION_NANODBC
        return EXT_VERSION_NANODBC;
#else
        return "v0.3.2";
#endif
    }
};



} // namespace duckdb