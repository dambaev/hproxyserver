{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE BangPatterns #-}
module TCPServer where

import Control.Concurrent
import Control.Exception
import GHC.IO.Exception
import Control.Concurrent.HEP as H
import Control.Concurrent.HEP.Syslog
import Network as N
import Foreign.Marshal.Alloc
import Foreign.Ptr
import Foreign.C.Types
import Network.Socket as S
import Control.Monad
import Control.Monad.Trans
import Control.Monad.Trans.Either
import GHC.IO.Handle
import System.IO
import Data.Typeable
import System.IO.Error
import System.Timeout

data SupervisorState = SupervisorState
    { serverPort:: PortID
    , supervisorStarter:: MBox SupervisorAnswer
    , supervisorWorker:: Pid
    , supervisorConnectionsCount:: Int
    }
    deriving Typeable
instance HEPLocalState SupervisorState

data WorkerState = WorkerState
    { workerHandle :: Maybe Handle
    , workerBuffer :: Ptr CChar
    , workerConsumer:: Maybe Handle
    , workerSocket :: Socket
    , workerTimeout:: Int
    }
    deriving Typeable
instance HEPLocalState WorkerState

data ClientState = ClientState
    { clientHandle :: Handle
    , clientBuffer :: Ptr CChar
    , clientConsumer:: Handle
    }
    deriving Typeable
instance HEPLocalState ClientState


data WorkerMessage = WorkerSetConsumer !Handle
    deriving Typeable
instance Message WorkerMessage

data WorkerFeedback = WorkerStarted !PortID
    deriving Typeable
instance Message WorkerFeedback

data ClientFeedback = ClientStarted !Handle
    deriving Typeable
instance Message ClientFeedback


data SupervisorCommand = SetWorkerConsumer !Handle
                       | StopServer
    deriving Typeable
instance Message SupervisorCommand

data SupervisorAnswer = ServerStarted !PortID

data ClientSupervisorCommand = StopClient
    deriving Typeable
instance Message ClientSupervisorCommand

{--
 - size for buffer for client/server connections
 -}
bufferSize = 64*1024

{-
 - start process, that:
 -  - listens on next available port, after base
 -  - waits for connectionsCount connections
 -  - timeouts waiting after timeoutVal
 -  - on client connection, it runs action onOpen
 -  - on connection shutdown, it runs action onClose
 -  - when it receives something, it runs receiveAction
 -}
startTCPServerBasePort:: PortID
                      -> Int
                      -> Int
                      -> (Int-> HEP ()) 
                      -> (Pid-> Handle-> HEP ()) 
                      -> HEP ()
                      -> HEP (Pid, PortID)
startTCPServerBasePort base connectionsCount timeoutVal receiveAction onOpen onClose = do
    !input <- liftIO newMBox
    sv <- spawn $! procWithBracket (serverSupInit base connectionsCount timeoutVal input receiveAction onOpen) 
        (serverSupShutdown onClose) $! proc $! serverSupervisor receiveAction onOpen timeoutVal
    -- wait while TCP server finds available port 
    ServerStarted !port <- liftIO $! receiveMBox input
    return (sv, port)

-- sets client connection for server
setConsumer:: Pid-> Handle-> HEP ()
setConsumer pid !h = do
    H.send pid $! SetWorkerConsumer h
    return ()

{-
 - initialization routine for servers' supervisor proc
 -}
serverSupInit:: PortID
             -> Int
             -> Int
             -> MBox SupervisorAnswer
             -> (Int-> HEP ()) 
             -> (Pid-> Handle-> HEP ()) 
             -> HEPProc
serverSupInit port connectionsCount timeoutVal starter receiveAction onOpen = do
    register "TCPserverSupervisor"
    me <- self
    -- spawn server proc and subscribe "me" to his messages
    pid <- spawn $! procWithSubscriber me $! 
        procWithBracket (serverInit port me timeoutVal) serverShutdown $! 
        proc $! serverWorker receiveAction (onOpen me)
    -- remember spawned proc as our subscribe
    addSubscribe pid
    -- remember state
    setLocalState $! Just $! SupervisorState
        { serverPort = port
        , supervisorStarter = starter
        , supervisorWorker = pid
        , supervisorConnectionsCount = connectionsCount
        }
    procRunning
    
{-
 - servers' process initialization routine
 -}
serverInit:: PortID
          -> Pid
          -> Int
          -> HEPProc
serverInit port svpid timeoutVal = do
    register "TCPserver"
    syslogInfo "worker started"
    -- try to listen on port, if it is busy, exception will be thrown
    -- that will be catched by Supervisor
    lsocket <- liftIO $! listenOn port
    -- allocate buffer for data
    buff <- liftIO $! mallocBytes bufferSize
    setLocalState $! Just $! WorkerState
        { workerHandle = Nothing
        , workerBuffer = buff
        , workerConsumer = Nothing
        , workerSocket = lsocket
        , workerTimeout = timeoutVal
        }
    H.send svpid $! WorkerStarted port
    procRunning
    
    
{-
 - servers' shutdown routine.
 -}
serverShutdown :: HEPProc
serverShutdown = do
    ls <- localState
    case ls of
        -- if there is no state at all
        Nothing -> procFinished
        -- if we have state, we should clean it
        Just some -> do
            liftIO $! do
                _ <- case workerHandle some of
                    Nothing-> return ()
                    Just wh -> do
                        closed <- hIsClosed wh
                        -- close server's connection handle
                        when (closed == False) $! do
                            hClose wh
                -- free buffer
                free (workerBuffer some)
                sClose (workerSocket some)
            _ <- case workerConsumer some of
                Nothing-> return ()
                Just h-> liftIO $! do
                    -- close client's connection handle
                    closed <- hIsClosed h
                    when (closed == False) $! do
                        hClose h
            procFinished

{-
 - main server's routine. it waits while something is received from
 - client's connection and calls serverIterate or it waits for client's
 - connection to establish, if it has no one yet
 -}
serverWorker:: (Int-> HEP ()) -> (Handle-> HEP ())->HEPProc
serverWorker receiveAction onOpen = do
    !mmsg <- receiveMaybe
    case mmsg of
        Just msg -> do
            case fromMessage msg of
                Just (WorkerSetConsumer !hout) -> do
                    Just ls <- localState
                    setLocalState $! Just $! ls { workerConsumer = Just hout}
                    procRunning
        Nothing-> serverIterate receiveAction onOpen

serverIterate receiveAction onOpen = do
    Just ls <- localState
    let !h = workerHandle ls
        !ptr = workerBuffer ls
        !consumer = workerConsumer ls
        !timeoutVal = workerTimeout ls
    case workerHandle ls of
        Nothing-> do
            case workerConsumer ls of
                Nothing-> do
                    maccept <- liftIO $! timeout (timeoutVal * 1000000)  $! 
                        N.accept (workerSocket ls)
                    case maccept of
                        Just (h, host, _) -> do
                            liftIO $! hSetBuffering h NoBuffering
                            liftIO $! hSetBinaryMode h True
                            syslogInfo $! "accepted connection from " ++ show host
                            setLocalState $! Just $! ls 
                                { workerHandle = Just h
                                }
                            onOpen h 
                            procRunning
                        _ -> procFinished
                Just hcons -> do
                    syslogInfo $! "closing consumer's handle on restart"
                    liftIO $! hClose hcons
                    setLocalState $! Just $! ls
                        { workerConsumer = Nothing
                        }
                    procRunning
        Just h -> case consumer of
            Nothing-> do
                liftIO $! yield >> threadDelay 500000
                procRunning
            Just hout -> do
                {-isready <- liftIO $! hWaitForInput h 1000
                if isready == False 
                    then do
                        procRunning
                    else do -}
                !mread <- liftIO $! timeout 1000000 $! hGetBufSome h ptr bufferSize
                case mread of
                    Nothing -> procRunning
                    Just 0 -> liftIO $! ioError $! mkIOError eofErrorType "no data received" Nothing Nothing
                    Just !read -> do
                        liftIO $! do
                            hPutBuf hout ptr read
                            hFlush hout
                        receiveAction read
                        procRunning


serverSupShutdown onClose = do
    onClose
    procFinished

serverSupervisor:: (Int-> HEP()) 
                -> (Pid-> Handle-> HEP())
                -> Int
                -> HEPProc
serverSupervisor receiveAction onOpen timeoutVal = do
    msg <- receive
    let handleChildLinkMessage:: Maybe LinkedMessage -> EitherT HEPProcState HEP HEPProcState
        handleChildLinkMessage Nothing = lift procRunning >>= right
        -- if worker is finished, then we shoud finish too
        handleChildLinkMessage (Just (ProcessFinished pid)) = do
            lift $! syslogInfo $! "supervisor: server thread exited"
            subscribed <- lift getSubscribed
            case subscribed of
                [] -> lift procFinished >>= left
                _ -> lift procRunning >>= left
        
        handleServiceMessage:: Maybe SupervisorMessage -> EitherT HEPProcState HEP HEPProcState
        handleServiceMessage Nothing = lift procRunning >>= right
        -- if worker's main proc has been failed, then we must either 
        -- finish it or, if connections-count > 1, then we should restart
        -- server and wait for another connection
        handleServiceMessage (Just (ProcWorkerFailure cpid e wstate outbox)) = do
            left =<< lift (do
                Just ls <- localState
                case fromException e of
                    Just (IOError{ioe_type = ResourceVanished}) -> do
                        syslogInfo $! "supervisor: server connection got ResourceVanished"
                        _ <- if supervisorConnectionsCount ls <= 1 
                            then procFinish outbox
                            else do
                                syslogInfo  $! "supervisor: awaiting next connection"
                                case procStateGetLocalState wstate of
                                    Nothing-> procFinish outbox
                                    Just oldstate_ -> do
                                        let newstate = case fromLocalState oldstate_ of
                                                Nothing -> Nothing
                                                Just oldstate -> Just $! toLocalState $! oldstate 
                                                    { workerHandle = Nothing
                                                    }
                                            newwstate = procStateSetLocalState wstate newstate
                                        procContinue outbox $! newwstate
                                        setLocalState $! Just $! ls 
                                            { supervisorConnectionsCount = 
                                                supervisorConnectionsCount ls- 1
                                            }
                        procRunning
                    Just (IOError{ioe_type = EOF}) -> do
                        syslogInfo "supervisor: server connection got EOF"
                        _ <- if supervisorConnectionsCount ls <= 1 
                            then procFinish outbox
                            else do
                                syslogInfo  $! "supervisor: awaiting next connection"
                                case procStateGetLocalState wstate of
                                    Nothing-> procFinish outbox
                                    Just oldstate_ -> do
                                        let newstate = case fromLocalState oldstate_ of
                                                Nothing -> Nothing
                                                Just oldstate -> Just $! toLocalState $! oldstate 
                                                    { workerHandle = Nothing
                                                    }
                                            newwstate = procStateSetLocalState wstate newstate
                                        procContinue outbox $! newwstate
                                        setLocalState $! Just $! ls 
                                            { supervisorConnectionsCount = 
                                                supervisorConnectionsCount ls- 1
                                            }
                        procRunning
                    _ -> do
                        syslogError $! "supervisor: server " ++ show cpid ++ 
                            " failed with: " ++ show e
                        procFinish outbox
                        procRunning
                )
        -- if worker's init was failed, we increase port's number and retry
        handleServiceMessage (Just (ProcInitFailure cpid e _ outbox)) = 
            left =<< lift ( do
                procFinish outbox
                Just ls <- localState
                let PortNumber port = serverPort ls
                    !newport = PortNumber (port + 1)
                me <- self
                syslogInfo $! "port " ++ show port ++ " is busy"
                pid <- spawn $! procWithSubscriber me $! 
                    procWithBracket (serverInit newport me timeoutVal) 
                        serverShutdown $! 
                    proc $! serverWorker receiveAction (onOpen me)
                addSubscribe pid
                setLocalState $! Just $! ls
                    { serverPort = newport
                    , supervisorWorker = pid
                    }
                procRunning
                )
        -- the impossible happend
        handleServiceMessage (Just (ProcShutdownFailure cpid e _ outbox)) = 
            left =<< lift ( do
                procFinish outbox
                syslogInfo $! "server shutdown failed with " ++ show e
                procRunning
                )

        handleWorkerFeedback:: Maybe WorkerFeedback-> EitherT HEPProcState HEP HEPProcState
        handleWorkerFeedback Nothing = right =<< lift procRunning
        handleWorkerFeedback (Just (WorkerStarted !port)) = do
            left =<< lift (do
                Just ls <- localState
                let !starter = supervisorStarter ls
                setLocalState $! Just $! ls {serverPort = port}
                liftIO$! sendMBox starter (ServerStarted port)
                procRunning
                )

        handleSupervisorCommand:: Maybe SupervisorCommand-> EitherT HEPProcState HEP HEPProcState
        handleSupervisorCommand Nothing = right =<< lift procRunning
        handleSupervisorCommand (Just (SetWorkerConsumer !handle)) = do
            left =<< lift (do
                Just ls <- localState
                let !worker = supervisorWorker ls
                H.send worker $! WorkerSetConsumer handle
                procRunning
                )
        handleSupervisorCommand (Just StopServer) = do
            left =<< lift (do
                subscribed <- getSubscribed
                forM subscribed $! \pid -> killProc pid
                procRunning
                )

    mreq <- runEitherT $! do
        handleChildLinkMessage $! fromMessage msg
        handleServiceMessage $! fromMessage msg 
        handleWorkerFeedback $! fromMessage msg
        handleSupervisorCommand $! fromMessage msg
    case mreq of
        Left some -> return some
        Right some -> return some

startTCPClient:: String
              -> PortID
              -> Handle
              -> (Int-> HEP ())
              -> HEP ()
              -> HEP (Handle, Pid)
startTCPClient addr port hserver receiveAction onInitFailed = do
    !inbox <- liftIO newMBox
    sv <- spawn $! procWithBracket (clientSupInit addr port inbox hserver receiveAction ) 
        procFinished $! -- (onClose >> procFinished) $!
        proc $! clientSupervisor onInitFailed
    answ <- liftIO $! receiveMBoxAfter 60000 inbox
    case answ of 
        Just (ClientStarted !h) -> return (h,sv)
        _ -> error "client was unable to connect"

clientSupInit:: String
             -> PortID
             -> MBox ClientFeedback
             -> Handle
             -> (Int-> HEP ())
             -> HEPProc
clientSupInit addr port outbox hserver receiveAction = do
    register "TCPClientSupervisor"
    me <- self
    pid <- spawn $! procWithSubscriber me $! 
        procWithBracket (clientInit addr port outbox hserver) 
        (clientShutdown hserver) $! proc $! clientWorker receiveAction
    addSubscribe pid
    procRunning

clientInit:: String-> PortID-> MBox ClientFeedback-> Handle-> HEPProc
clientInit addr port outbox consumer = do
    register "TCPClient"
    buff <- liftIO $! mallocBytes bufferSize
    let ls = ClientState
            { clientBuffer = buff
            , clientConsumer = consumer
            }
    setLocalState $! Just $! ls
    h <- liftIO $! connectTo addr port 
    liftIO $! hSetBuffering h NoBuffering
    liftIO $! hSetBinaryMode h True
    setLocalState $! Just $! ls
        { clientHandle = h
        }
    liftIO $! sendMBox outbox $! ClientStarted h
    procRunning

clientShutdown:: Handle-> HEPProc
clientShutdown hserver = do
    sclosed <- liftIO $! hIsClosed hserver
    when (sclosed == False) $! liftIO $! hClose hserver
    ls <- localState
    case ls of
        Nothing-> procFinished
        Just some -> do
            liftIO $! do
                cclosed <- hIsClosed (clientHandle some)
                when (cclosed == False) $! do
                    hClose (clientHandle some)
                free (clientBuffer some)
            procFinished

clientWorker:: (Int-> HEP ()) -> HEPProc
clientWorker receiveAction = do
    Just ls <- localState
    let !h = clientHandle ls
        !ptr = clientBuffer ls
        !consumer = clientConsumer ls
    {- isready <- liftIO $! hWaitForInput h 1000
    if isready == False 
        then do
            procRunning
        else do -}
    !mread <- liftIO $! timeout 1000000 $! hGetBufSome h ptr bufferSize
    case mread of
        Nothing -> procRunning
        Just 0 -> procFinished
        Just !read -> do
            liftIO $! do
                hPutBuf consumer ptr read
                hFlush consumer
            receiveAction read
            procRunning
            
stopTCPServer:: Pid-> HEP ()
stopTCPServer pid = do
    H.send pid StopServer
    
stopTCPClient:: Pid-> HEP ()
stopTCPClient pid = do
    H.send pid $! StopClient

clientSupervisor:: HEP () -> HEPProc
clientSupervisor onInitFailure = do
    msg <- receive
    let handleChildLinkMessage:: Maybe LinkedMessage -> EitherT HEPProcState HEP HEPProcState
        handleChildLinkMessage Nothing = lift procRunning >>= right
        handleChildLinkMessage (Just (ProcessFinished pid)) = do
            lift $! syslogInfo $! "supervisor: client thread exited "
            subscribed <- lift getSubscribed
            case subscribed of
                [] -> lift procFinished >>= left
                _ -> lift procRunning >>= left
        
        handleServiceMessage:: Maybe SupervisorMessage -> EitherT HEPProcState HEP HEPProcState
        handleServiceMessage Nothing = right =<< lift procRunning
        handleServiceMessage (Just (ProcWorkerFailure cpid e _ outbox)) = do
            left =<< lift (do
                case fromException e of
                    Just (IOError{ioe_type = ResourceVanished}) -> do
                        syslogInfo "supervisor: client connection got ResourceVanished"
                        procFinish outbox
                        procFinished
                    Just (IOError{ioe_type = EOF}) -> do
                        syslogInfo "supervisor: client connection got EOF"
                        procFinish outbox
                        procRunning
                    _ -> do
                        syslogError $! "supervisor: client " ++ show cpid ++ 
                            " failed with: " ++ show e
                        procFinish outbox
                        procRunning
                )
        handleServiceMessage (Just (ProcInitFailure cpid e _ outbox)) = do
            left =<< lift (do
                liftIO $! putStrLn $! "ERROR: " ++ show e
                syslogError $! "supervisor: client init " ++ show cpid ++ 
                    " failed with: " ++ show e
                procFinish outbox
                onInitFailure
                procRunning
                )
        handleClientSupervisorCommand:: Maybe ClientSupervisorCommand
                                     -> EitherT HEPProcState HEP HEPProcState
        handleClientSupervisorCommand Nothing =right =<< lift procRunning
        handleClientSupervisorCommand (Just StopClient) = left =<< lift
            ( do
                workers <- getSubscribed
                forM workers $! \pid -> do
                    syslogInfo $! "killing client " ++ show pid
                    killProc pid
                procRunning
            )
    mreq <- runEitherT $! do
        handleChildLinkMessage $! fromMessage msg
        handleServiceMessage $! fromMessage msg 
        handleClientSupervisorCommand $! fromMessage msg
        
    case mreq of
        Left some -> return some
        Right some -> return some
    
