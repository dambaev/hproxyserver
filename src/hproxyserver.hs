{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveDataTypeable #-}
module Main where


import Control.Concurrent.HEP as H
import Control.Concurrent.HEP.Syslog
import Control.Concurrent
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
import Network
import Config
import TCPServer
import System.IO

data MainState = MainState
    { proxySession:: ProxySession
    , mainServer:: Maybe Pid
    , mainClient :: Maybe Pid
    , mainConfig:: Maybe Config
    , mainRead:: Integer
    , mainWrote:: Integer
    }
    deriving (Typeable)
instance HEPLocalState MainState

defaultMainState = MainState
    { mainServer = Nothing
    , mainClient = Nothing
    , mainConfig = Nothing
    , mainRead = 0
    , mainWrote = 0
    }

data MainFlag = FlagDestination Destination
    deriving Show

data MainMessage = MainServerReceived Int
                 | MainClientReceived Int
                 | MainServerConnection !Handle
    deriving Typeable
instance Message MainMessage
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

main = withSocketsDo $! runHEPGlobal $! procWithSupervisor (H.proc superLogAndExit) $! 
    procWithBracket mainInit mainShutdown $! H.proc $! do
        Just ls <- localState
        let Just config = mainConfig ls
            !timeout = configConnectionTimeout config
        mmsg <- receiveAfter (timeout * 1000)
        case mmsg of
            Nothing-> procFinished
            Just msg -> case fromMessage msg of
                Nothing-> procRunning
                Just (MainServerReceived !read) -> do
                    let !old = mainRead ls
                    setLocalState $! Just $! ls 
                        { mainRead = old + (fromIntegral read)
                        }
                    procRunning
                Just (MainClientReceived !read) -> do
                    let !old = mainRead ls
                    setLocalState $! Just $! ls 
                        { mainWrote = old + (fromIntegral read)
                        }
                    procRunning
                Just (MainServerConnection hserver) -> do
                    Just ls <- localState
                    me <- self
                    let session = proxySession ls
                        DestinationAddrPort (IPAddress addr) port =     
                            sessionDestination session
                        Just server = mainServer ls
                    syslogInfo $! "starting client to " ++ 
                        show addr ++ ":" ++ show port
                    (!hclient, clientpid) <- 
                        startTCPClient addr 
                            (PortNumber $! fromIntegral port) 
                            hserver
                            (\x-> H.send me $! MainClientReceived x)
                    setConsumer server hclient
                    syslogInfo "client started"
                    procRunning
        

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
            liftIO $! putStrLn $! "ERROR: " ++ show e
            lift $! syslogError $! "supervisor: worker " ++ show cpid ++ 
                " failed with: " ++ show e ++ ". It will be recovered"
            lift $! procFinish outbox
            lift procRunning >>= left
        handleServiceMessage (Just (ProcInitFailure cpid e _ outbox)) = do
            liftIO $! putStrLn $! "ERROR: " ++ show e
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
    syslogInfo "loading config"
    config <- liftIO $! loadConfig "/etc/hproxy/config"
    syslogInfo "generating ProxySession info"
    !session <- generateProxySession
    syslogInfo "loading rules"
    !rules <- liftIO $! parseRuleDir (configRulesDir config)
    syslogInfo $! "loaded " ++ (show $ length rules) ++ " rule-files"
    
    let !matched = matchSessionRules session rules
    case matched of
        Nothing-> do
            error "no matching rule found"
        Just (!fname, !line, !rule) -> do
            syslogInfo $! "matched rule (" ++ fname ++ ":" ++ 
                show line ++ "): " ++ show rule
            case rulePermission rule of
                RuleDeny -> error "denied"
                RuleAllow -> do
                    me <- self
                    (servpid, (PortNumber port)) <- 
                        startTCPServerBasePort 
                            (PortNumber $! fromIntegral $! configTCPPortsBase config)
                            ( \x-> H.send me $! MainServerReceived x)
                            (\x-> H.send me $! MainServerConnection x)
                    liftIO $! putStrLn $! "OK " ++ show port
                    syslogInfo $! "TCP server started on port " ++ 
                        show port
                    Just ls <- localState
                    setLocalState $! Just $! ls 
                        { mainServer = Just servpid
                        , mainConfig = Just config
                        }
                    procRunning
    
    
mainShutdown:: HEPProc
mainShutdown = do
    ls <- localState
    _ <- case ls of
        Nothing-> return ()
        Just state -> do
            let readed = case mainRead state of
                    some | some >= 1024 * 1048576 -> 
                        show (fromIntegral some / (1024 * 1048576)) ++ " GiB"
                    some | some >= 1048576 -> 
                        show (fromIntegral some / 1048576) ++ " MiB"
                    some | some >= 1024 -> 
                        show (fromIntegral some / 1024) ++ " KiB"
                    some -> show some ++ " B"
                wrote = case mainWrote state of
                    some | some >= 1024 * 1048576 -> 
                        show (fromIntegral some / (1024 * 1048576)) ++ " GiB"
                    some | some >= 1048576 -> 
                        show (fromIntegral some / 1048576) ++ " MiB"
                    some | some >= 1024 -> 
                        show (fromIntegral some / 1024) ++ " KiB"
                    some -> show some ++ " B"
                total = case mainRead state + mainWrote state of
                    some | some >= 1024 * 1048576 -> 
                        show (fromIntegral some / (1024 * 1048576)) ++ " GiB"
                    some | some >= 1048576 -> 
                        show (fromIntegral  some / 1048576) ++ " MiB"
                    some | some >= 1024 -> 
                        show (fromIntegral  some / 1024) ++ " KiB"
                    some -> show some ++ " B"
            syslogInfo $! "session closed. readed: " ++ readed ++ 
                ", wrote: " ++ wrote ++ ", total: " ++ total
    stopSyslog
    procFinished

    

generateProxySession:: HEP ProxySession
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
    let ret = ProxySession
            { sessionUserSID = usersid
            , sessionGroupsSIDs = groups
            , sessionDate = date
            , sessionWeekDay = weekday
            , sessionTime = time
            , sessionDestination = dest
            }
    setLocalState $! Just $! defaultMainState
        { proxySession = ret
        }
    return ret

    
