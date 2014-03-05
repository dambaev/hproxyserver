module Config where

data Config = Config
    { configRulesDir :: String
    , configNotifyCMD:: String
    , configTCPPortsBase:: Int
    , configConnectionTimeout:: Int
    }
    deriving Show


defaultConfig = Config
    { configRulesDir = "/etc/hproxy/rules.d/"
    , configNotifyCMD = "proxynotify"
    , configTCPPortsBase = 10000
    , configConnectionTimeout = 1200
    }

-- dummy config loader
loadConfig:: String-> IO Config
loadConfig fname = return defaultConfig
