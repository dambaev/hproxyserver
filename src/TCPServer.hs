{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE BangPatterns #-}
module TCPServer where

import Control.Concurrent.HEP
import Control.Concurrent.HEP.Syslog
import Network
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
    }
    deriving Typeable
instance HEPLocalState SupervisorState

data WorkerState = WorkerState
    { workerHandle :: Handle
    , workerBuffer :: Ptr CChar
    }
    deriving Typeable
instance HEPLocalState WorkerState

bufferSize = 64*1024

startTCPServerBasePort:: PortID-> HEP Pid
startTCPServerBasePort base = do
    sv <- spawn $! procWithBracket (serverSupInit base) serverSupShutdown $! proc serverSupervisor
    return sv
    
    
serverSupInit:: PortID-> HEPProc
serverSupInit port = do
    setLocalState $! Just $! SupervisorState{serverPort = port}
    me <- self
    spawn $! procWithSubscriber me $! 
        procWithBracket (serverInit port me) serverShutdown $! 
        proc $! serverWorker
    procRunning
    
    
serverInit:: PortID-> Pid-> HEPProc
serverInit port svpid = do
    lsocket <- liftIO $! listenOn port
    (_socket, SockAddrInet _ addr) <- liftIO $! S.accept lsocket
    !host <- liftIO $! inet_ntoa addr
    syslogInfo $! "accepted connection from " ++ host
    h <- liftIO $! socketToHandle _socket ReadWriteMode
    buff <- liftIO $! mallocBytes bufferSize
    setLocalState $! Just $! WorkerState
        { workerHandle = h
        , workerBuffer = buff
        }
    liftIO $! sClose lsocket
    syslogInfo "worker started"
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


serverWorker:: HEPProc
serverWorker = do
    msg <- receiveMaybe
    case msg of
        Nothing-> do
            Just ls <- localState
            let !h = workerHandle ls
                !ptr = workerBuffer ls
            !read <- liftIO $! hGetBuf h ptr bufferSize
            case read of
                0 -> procRunning
                _ -> do
                    syslogInfo $! "readed " ++ show read ++ " bytes"
                    procRunning


serverSupShutdown = procFinished

serverSupervisor:: HEPProc
serverSupervisor = do
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
            lift $! procFinish outbox
            Just ls <- lift localState
            let PortNumber port = serverPort ls
                !newport = PortNumber (port + 1)
            lift $! setLocalState $! Just $! ls{ serverPort = newport}
            me <- lift $! self
            pid <- lift $! spawn $! procWithSubscriber me $! 
                procWithBracket (serverInit newport me) serverShutdown $! 
                proc $! serverWorker
            lift $! addSubscribe pid
            lift procRunning >>= left
    mreq <- runEitherT $! do
        handleChildLinkMessage $! fromMessage msg
        handleServiceMessage $! fromMessage msg 
        
    case mreq of
        Left some -> return some
        Right some -> return some

