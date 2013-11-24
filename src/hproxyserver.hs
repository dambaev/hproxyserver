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
import Control.Monad.Trans.Either
import Network.AD.SID
import System.Environment
import System.Console.GetOpt

data MainState = MainState
    { uuid:: UUID -- session UUID
    , proxySession:: ProxySession
    }
    deriving (Eq, Show, Typeable)
instance HEPLocalState MainState

data MainFlag = FlagDestination String
    deriving Show

options :: [OptDescr MainFlag]
options = 
    [ Option ['d']     ["dest"]  (ReqArg FlagDestination "DEST") "destination addr:port"
    ]

getMainOptions:: [String]-> IO [MainFlag]
getMainOptions argv =
    case getOpt Permute options argv of
        (!o,n,[]  ) -> return o
        (_,_,errs) -> ioError (userError (concat errs ++ usageInfo header options))
            where header = "Usage: hproxyserver [OPTION...]"

main = runHEPGlobal $! procWithSupervisor (H.proc superLogAndExit) $! 
    procWithBracket mainInit mainShutdown $! H.proc $! do
        procFinished

superLogAndExit:: HEPProc
superLogAndExit = do
    msg <- receive
    let handleChildLinkMessage:: Maybe LinkedMessage -> EitherT HEPProcState HEP HEPProcState
        handleChildLinkMessage Nothing = lift procRunning >>= right
        handleChildLinkMessage (Just (ProcessFinished pid)) = do
            lift $! syslogInfo $! "supervisor: spotted client exit " ++ show pid
            subscribed <- lift getSubscribed
            case subscribed of
                [] -> lift procFinished >>= left
                _ -> lift procRunning >>= left
        
        handleServiceMessage:: Maybe SupervisorMessage -> EitherT HEPProcState HEP HEPProcState
        handleServiceMessage Nothing = lift procRunning >>= right
        handleServiceMessage (Just (ProcWorkerFailure cpid e _ outbox)) = do
            lift $! syslogError $! "supervisor: worker " ++ show cpid ++ 
                " failed with: " ++ show e ++ ". It will be recovered"
            lift $! procFinish outbox
            lift procRunning >>= left
        handleServiceMessage (Just (ProcInitFailure cpid e _ outbox)) = do
            lift $! syslogError $! "supervisor: init of " ++ show cpid ++ 
                " failed with: " ++ show e
            lift $! procFinish outbox
            lift procRunning >>= left
    mreq <- runEitherT $! do
        handleChildLinkMessage $! fromMessage msg
        handleServiceMessage $! fromMessage msg 
        
    case mreq of
        Left some -> return some
        Right some -> return some
    

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
    eusersid <- liftIO $! getCurrentUserSID
    case eusersid of
        Left e -> liftIO $! ioError $! userError e
        Right usersid -> do
            syslogInfo $! "current user SID " ++ show usersid
            Right groups <- liftIO $! getCurrentGroupsSIDs usersid
            syslogInfo $! "current user's  groups' SID " ++ show groups
            args <- liftIO $! getArgs >>= getMainOptions
            liftIO $! print "args"
            liftIO $! print args
            return ()

