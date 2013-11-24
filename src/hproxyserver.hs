{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveDataTypeable #-}
module Main where


import Control.Concurrent.HEP as H
import Control.Concurrent.HEP.Syslog
import System.Process
import Data.Typeable
import Data.UUID
import Data.UUID.V4
import Data.HProxy.Session
import Data.HProxy.Rules
import Control.Monad.Trans
import Network.AD.SID

data MainState = MainState
    { uuid:: UUID -- session UUID
    , proxySession:: ProxySession
    }
    deriving (Eq, Show, Typeable)
instance HEPLocalState MainState

main = runHEPGlobal $! procWithBracket mainInit mainShutdown $! H.proc $! do
    procFinished



mainInit:: HEPProc
mainInit = do
    startSyslog "hproxyserver"
    generateProxySession
    procRunning
    
    
mainShutdown:: HEPProc
mainShutdown = do
    stopSyslog
    procFinished

    

generateProxySession:: HEP ()
generateProxySession = do
    !myuuid <- liftIO $! nextRandom
    syslogInfo $! "session uuid: " ++ show myuuid
    usersid <- liftIO $! getCurrentUserSID
    syslogInfo $! "current user SID " ++ show usersid

