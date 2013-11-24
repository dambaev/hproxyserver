{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE BangPatterns #-}
module TCPServer where

import Control.Concurrent
import Control.Concurrent.HEP as H
import Control.Concurrent.HEP.Syslog
import Network as N
import Foreign.Marshal.Alloc
import Foreign.Ptr
import Foreign.C.Types
import Network.Socket as S
import Control.Monad.Trans
import Control.Monad.Trans.Either
import GHC.IO.Handle
import System.IO
import Data.Typeable

data SupervisorState = SupervisorState
    { serverPort:: PortID
    , supervisorStarter:: MBox SupervisorAnswer
    , supervisorWorker:: Pid
    }
    deriving Typeable
instance HEPLocalState SupervisorState

data WorkerState = WorkerState
    { workerHandle :: Handle
    , workerBuffer :: Ptr CChar
    , workerConsumer:: Maybe Handle
    }
    deriving Typeable
instance HEPLocalState WorkerState

data WorkerMessage = WorkerStop
                   | WorkerSetConsumer !Handle
    deriving Typeable
instance Message WorkerMessage

data WorkerFeedback = WorkerStarted !PortID
    deriving Typeable
instance Message WorkerFeedback

data SupervisorCommand = SetWorkerConsumer !Handle
    deriving Typeable
instance Message SupervisorCommand

data SupervisorAnswer = ServerStarted !PortID

bufferSize = 64*1024

startTCPServerBasePort:: PortID
                      -> (Int-> HEP ()) 
                      -> (Handle-> HEP ()) 
                      -> HEP (Pid, PortID)
startTCPServerBasePort base receiveAction onOpen = do
    !input <- liftIO newMBox
    sv <- spawn $! procWithBracket (serverSupInit base input receiveAction onOpen) 
        serverSupShutdown $! proc $! serverSupervisor receiveAction onOpen 
    ServerStarted !port <- liftIO $! receiveMBox input
    return (sv, port)

setConsumer:: Pid-> Handle-> HEP ()
setConsumer pid !h = do
    H.send pid $! SetWorkerConsumer h
    return ()

    
serverSupInit:: PortID
             -> MBox SupervisorAnswer
             -> (Int-> HEP ()) 
             -> (Handle-> HEP ()) 
             -> HEPProc
serverSupInit port starter receiveAction onOpen = do
    me <- self
    pid <- spawn $! procWithSubscriber me $! 
        procWithBracket (serverInit port me onOpen) serverShutdown $! 
        proc $! serverWorker receiveAction 
    addSubscribe pid
    setLocalState $! Just $! SupervisorState
        { serverPort = port
        , supervisorStarter = starter
        , supervisorWorker = pid
        }
    procRunning
    
    
serverInit:: PortID
          -> Pid
          -> (Handle-> HEP ())
          -> HEPProc
serverInit port svpid onOpen = do
    syslogInfo "worker started"
    lsocket <- liftIO $! listenOn port
    H.send svpid $! WorkerStarted port
    (h, host, _) <- liftIO $! N.accept lsocket
    liftIO $! hSetBuffering h NoBuffering
    liftIO $! hSetBinaryMode h True
    syslogInfo $! "accepted connection from " ++ show host
    buff <- liftIO $! mallocBytes bufferSize
    setLocalState $! Just $! WorkerState
        { workerHandle = h
        , workerBuffer = buff
        , workerConsumer = Nothing
        }
    onOpen h 
    liftIO $! sClose lsocket
    procRunning
    
    

serverShutdown :: HEPProc
serverShutdown = do
    ls <- localState
    case ls of
        Nothing -> procFinished
        Just some -> do
            liftIO $! hClose (workerHandle some)
            liftIO $! free (workerBuffer some)
            procFinished


serverWorker:: (Int-> HEP ()) -> HEPProc
serverWorker receiveAction = do
    mmsg <- receiveMaybe
    case mmsg of
        Just msg -> do
            case fromMessage msg of
                Just WorkerStop -> procFinished
                Just (WorkerSetConsumer !hout) -> do
                    Just ls <- localState
                    setLocalState $! Just $! ls { workerConsumer = Just hout}
                    procRunning
        Nothing-> do
            Just ls <- localState
            let !h = workerHandle ls
                !ptr = workerBuffer ls
                !consumer = workerConsumer ls
            case consumer of
                Nothing-> do
                    liftIO $! yield >> threadDelay 500000
                    procRunning
                Just hout -> do
                    !read <- liftIO $! hGetBufSome h ptr bufferSize
                    case read of
                        0 -> procFinished
                        _ -> do
                            liftIO $! hPutBuf hout ptr read
                            receiveAction read
                            procRunning


serverSupShutdown = procFinished

serverSupervisor:: (Int-> HEP()) 
                -> (Handle-> HEP())
                -> HEPProc
serverSupervisor receiveAction onOpen = do
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
        handleServiceMessage (Just (ProcInitFailure cpid e _ outbox)) = 
            left =<< lift ( do
                procFinish outbox
                Just ls <- localState
                let PortNumber port = serverPort ls
                    !newport = PortNumber (port + 1)
                me <- self
                syslogInfo $! "port " ++ show port ++ " is busy"
                pid <- spawn $! procWithSubscriber me $! 
                    procWithBracket (serverInit newport me onOpen) 
                        serverShutdown $! 
                    proc $! serverWorker receiveAction
                addSubscribe pid
                setLocalState $! Just $! ls
                    { serverPort = newport
                    , supervisorWorker = pid
                    }
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

    mreq <- runEitherT $! do
        handleChildLinkMessage $! fromMessage msg
        handleServiceMessage $! fromMessage msg 
        handleWorkerFeedback $! fromMessage msg
        handleSupervisorCommand $! fromMessage msg
    case mreq of
        Left some -> return some
        Right some -> return some

