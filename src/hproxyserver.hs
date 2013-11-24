{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveDataTypeable #-}
module Main where


import Control.Concurrent.HEP as H
import Control.Concurrent.HEP.Syslog
import System.Process
import Data.Typeable
import Data.UUID
import Data.UUID.V4
import Data.Time.Clock
import Data.Time.Calendar
import Data.Time.Calendar.WeekDate
import Data.Time.LocalTime
import Data.HProxy.Session
import Data.HProxy.Rules
import Control.Monad.Trans
import Control.Monad.Trans.Either
import Network.AD.SID
import System.Environment
import System.Console.GetOpt
import Text.ParserCombinators.Parsec


data MainState = MainState
    { proxySession:: ProxySession
    }
    deriving (Eq, Show, Typeable)
instance HEPLocalState MainState

data MainFlag = FlagDestination Destination
    deriving Show

options :: [OptDescr MainFlag]
options = 
    [ Option ['d']     ["dest"]  (ReqArg getDestFlag "addr:port") "destination"
    ]
    
getDestFlag:: String-> MainFlag
getDestFlag str = let parsed = parse parseAddrPort "arg" ("addr "++str)
    in case parsed of
        Right dest -> FlagDestination $! dest
        Left some -> error $! show some

getMainOptions:: [String]-> IO [MainFlag]
getMainOptions argv =
    case getOpt Permute options argv of
        ([],_,_) -> ioError (userError (usageInfo header options))
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
    !myuuid <- liftIO $! nextRandom
    startSyslog $! "hproxyserver-" ++ show myuuid
    generateProxySession
    procRunning
    
    
mainShutdown:: HEPProc
mainShutdown = do
    stopSyslog
    procFinished

    

generateProxySession:: HEP ()
generateProxySession = do
    utctime <- liftIO $! getCurrentTime
    tz <- liftIO $! getCurrentTimeZone
    let !localtime = utcToLocalTime tz utctime
    syslogInfo $! "now is " ++ show localtime
    
    let time = TimeHHMM (todHour tod) (todMin tod)
        tod = localTimeOfDay localtime
        date = let (y, m, d) = toGregorian lday
               in DateYYYYMMDD (fromIntegral y) m d
        lday = (localDay localtime)
        (_, _, !weekday) = toWeekDate lday
    syslogInfo $! "week day is " ++ show weekday
    FlagDestination dest <- liftIO $! getArgs >>= getMainOptions >>= return . head
    syslogInfo $! "destination: " ++ show dest
    (!username, !usersid) <- liftIO $! getCurrentUserSID >>= \x-> case x of
        Left e  -> liftIO $! ioError $! userError e
        Right some -> return some
    syslogInfo $! "current user " ++ show username
    syslogInfo $! "current user SID " ++ show usersid
    Right groups <- liftIO $! getCurrentGroupsSIDs usersid
    syslogInfo $! "current user's  groups' SID " ++ show groups
    setLocalState $! Just $! MainState
        { proxySession = ProxySession
            { sessionUserSID = usersid
            , sessionGroupsSIDs = groups
            , sessionDate = date
            , sessionWeekDay = weekday
            , sessionTime = time
            , sessionDestination = dest
            }
        }
    return ()

