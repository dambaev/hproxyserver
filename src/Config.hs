module Config where

data Config = Config
    { configRulesDir :: String
    , configTCPPortsBase:: Int
    , configConnectionTimeout:: Int
    }


defaultConfig = Config
    { configRulesDir = "/etc/hproxy/rules.d/"
    , configTCPPortsBase = 10000
    , configConnectionTimeout = 20
    }


loadConfig:: String-> IO Config
loadConfig fname = return defaultConfig
