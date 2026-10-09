-- |
-- Module      :  Distribution.Client.JobControl
-- Copyright   :  (c) Duncan Coutts 2012
-- License     :  BSD-like
--
-- Maintainer  :  cabal-devel@haskell.org
-- Stability   :  provisional
-- Portability :  portable
--
-- A job control concurrency abstraction
module Distribution.Client.JobControl
  ( JobControl
  , JobPriority (..)
  , defaultJobPriority
  , newSerialJobControl
  , newParallelJobControl
  , newSemaphoreJobControl
  , spawnJob
  , spawnJobWithPriority
  , collectJob
  , remainingJobs
  , cancelJobs
  , cleanupJobControl
  , jobControlSemaphore
  , JobLimit
  , newJobLimit
  , withJobLimit
  , Lock
  , newLock
  , criticalSection

    -- * Higher level utils
  , newJobControlFromParStrat
  , withJobControl
  , mapConcurrentWithJobs
  ) where

import Distribution.Client.Compat.Prelude
import Prelude ()

import Control.Concurrent (forkIO, forkIOWithUnmask, threadDelay)
import Control.Concurrent.MVar
import Control.Concurrent.STM (STM, TVar, atomically, check, modifyTVar', newTVarIO, readTVar, retry, writeTVar)
import Control.Concurrent.STM.TChan
import Control.Exception (bracket, bracket_, finally, mask_, try)
import Control.Monad (forever, replicateM_)
import qualified Data.Map.Strict as Map
import Data.Ord (Down (..))
import Distribution.Client.Compat.Semaphore
import Distribution.Client.Utils (numberOfProcessors)
import Distribution.Compat.Stack
import Distribution.Simple.Compiler
import Distribution.Simple.Utils
import Distribution.Types.ParStrat
import System.Semaphore

-- | A simple concurrency abstraction. Jobs can be spawned and can complete
-- in any order. This allows both serial and parallel implementations.
data JobControl m a = JobControl
  { spawnJobWithPriority :: JobPriority -> m a -> m ()
  -- ^ Add a new job to the pool of jobs. Job controls that run jobs
  -- concurrently start the queued job of highest priority first; the serial
  -- job control ignores the priority.
  , collectJob :: m a
  -- ^ Wait until one job is complete
  , remainingJobs :: m Bool
  -- ^ Returns True if there are any outstanding jobs
  -- (ie spawned but yet to be collected)
  , cancelJobs :: m ()
  -- ^ Try to cancel any outstanding but not-yet-started jobs.
  -- Call 'remainingJobs' after this to find out if any jobs are left
  -- (ie could not be cancelled).
  , cleanupJobControl :: m ()
  -- ^ cleanup any resources created by the JobControl, intended to be used
  -- as the finaliser for `bracket`.
  , jobControlSemaphore :: Maybe SemaphoreIdentifier
  -- ^ Identifier of the semaphore which can be used to control parallelism,
  -- if one is available for that job control type.
  }

-- | Scheduling priority of a job. A job of higher priority starts first; among
-- queued jobs of equal priority, the first spawned starts first.
newtype JobPriority = JobPriority Int
  deriving (Eq, Ord, Show)

-- | The priority of jobs added with 'spawnJob'.
defaultJobPriority :: JobPriority
defaultJobPriority = JobPriority 0

-- | Add a new job to the pool of jobs, at 'defaultJobPriority'.
spawnJob :: JobControl m a -> m a -> m ()
spawnJob jobCtl = spawnJobWithPriority jobCtl defaultJobPriority

-- | Make a 'JobControl' that executes all jobs serially and in order.
-- It only executes jobs on demand when they are collected, not eagerly.
--
-- Cancelling will cancel /all/ jobs that have not been collected yet.
newSerialJobControl :: IO (JobControl IO a)
newSerialJobControl = do
  qVar <- newTChanIO
  return
    JobControl
      { spawnJobWithPriority = const (spawn qVar)
      , collectJob = collect qVar
      , remainingJobs = remaining qVar
      , cancelJobs = cancel qVar
      , cleanupJobControl = return ()
      , jobControlSemaphore = Nothing
      }
  where
    spawn :: TChan (IO a) -> IO a -> IO ()
    spawn qVar job = atomically $ writeTChan qVar job

    collect :: TChan (IO a) -> IO a
    collect qVar =
      join $ atomically $ readTChan qVar

    remaining :: TChan (IO a) -> IO Bool
    remaining qVar = fmap not $ atomically $ isEmptyTChan qVar

    cancel :: TChan (IO a) -> IO ()
    cancel qVar = do
      _ <- atomically $ readAllTChan qVar
      return ()

-- | Make a 'JobControl' that eagerly executes jobs in parallel, with a given
-- maximum degree of parallelism.
--
-- Cancelling will cancel jobs that have not yet begun executing, but jobs
-- that have already been executed or are currently executing cannot be
-- cancelled.
newParallelJobControl :: WithCallStack (Int -> IO (JobControl IO a))
newParallelJobControl n
  | n < 1 || n > 1000 =
      error $ "newParallelJobControl: not a sensible number of jobs: " ++ show n
newParallelJobControl maxJobLimit = do
  inqVar <- newTVarIO emptyJobQueue
  outqVar <- newTChanIO
  countVar <- newTVarIO 0
  replicateM_ maxJobLimit $
    forkIO $
      worker inqVar outqVar
  return
    JobControl
      { spawnJobWithPriority = spawn inqVar countVar
      , collectJob = collect outqVar countVar
      , remainingJobs = remaining countVar
      , cancelJobs = cancel inqVar countVar
      , cleanupJobControl = return ()
      , jobControlSemaphore = Nothing
      }
  where
    worker :: TVar (JobQueue (IO a)) -> TChan (Either SomeException a) -> IO ()
    worker inqVar outqVar =
      forever $ do
        job <- atomically $ takeJob inqVar
        res <- try job
        atomically $ writeTChan outqVar res

    spawn :: TVar (JobQueue (IO a)) -> TVar Int -> JobPriority -> IO a -> IO ()
    spawn inqVar countVar prio job =
      atomically $ do
        modifyTVar' countVar (+ 1)
        modifyTVar' inqVar (pushJob prio job)

    collect :: TChan (Either SomeException a) -> TVar Int -> IO a
    collect outqVar countVar = do
      res <- atomically $ do
        modifyTVar' countVar (subtract 1)
        readTChan outqVar
      either throwIO return res

    remaining :: TVar Int -> IO Bool
    remaining countVar = fmap (/= 0) $ atomically $ readTVar countVar

    cancel :: TVar (JobQueue (IO a)) -> TVar Int -> IO ()
    cancel inqVar countVar =
      atomically $ do
        n <- dropJobs inqVar
        modifyTVar' countVar (subtract n)

-- | Jobs waiting to start, ordered by descending priority, then by the
-- sequence number given when spawned.
data JobQueue a = JobQueue
  { jobQueueNextSeqNo :: !Int
  , jobQueueJobs :: !(Map.Map (Down JobPriority, Int) a)
  }

emptyJobQueue :: JobQueue a
emptyJobQueue = JobQueue 0 Map.empty

nullJobQueue :: JobQueue a -> Bool
nullJobQueue = Map.null . jobQueueJobs

pushJob :: JobPriority -> a -> JobQueue a -> JobQueue a
pushJob prio job (JobQueue n jobs) =
  JobQueue{jobQueueNextSeqNo = n + 1, jobQueueJobs = Map.insert (Down prio, n) job jobs}

tryTakeJob :: TVar (JobQueue a) -> STM (Maybe a)
tryTakeJob qVar = do
  q <- readTVar qVar
  for (Map.minView (jobQueueJobs q)) $ \(job, jobs') ->
    job <$ writeTVar qVar q{jobQueueJobs = jobs'}

-- | Take the next job, blocking while the queue is empty.
takeJob :: TVar (JobQueue a) -> STM a
takeJob qVar = maybe retry return =<< tryTakeJob qVar

-- | Drop all queued jobs, returning how many there were.
dropJobs :: TVar (JobQueue a) -> STM Int
dropJobs qVar = do
  q <- readTVar qVar
  writeTVar qVar q{jobQueueJobs = Map.empty}
  return (Map.size (jobQueueJobs q))

readAllTChan :: TChan a -> STM [a]
readAllTChan qvar = go []
  where
    go xs = do
      mx <- tryReadTChan qvar
      case mx of
        Nothing -> return (reverse xs)
        Just x -> go (x : xs)

-- | Make a 'JobControl' where the parallelism is controlled by a semaphore.
--
-- This uses the GHC -jsem option to allow GHC to take additional semaphore slots
-- if we are not using them all.
newSemaphoreJobControl :: WithCallStack (Verbosity -> Int -> IO (JobControl IO a))
newSemaphoreJobControl _ n
  | n < 1 || n > 1000 =
      error $ "newParallelJobControl: not a sensible number of jobs: " ++ show n
newSemaphoreJobControl verbosity maxJobLimit = do
  mbServer <- freshSemaphore "cabal_semaphore" maxJobLimit
  case mbServer of
    Left err -> do
      warn verbosity $
        "Failed to create semaphore: "
          ++ show err
          ++ "; falling back to -j"
          ++ show maxJobLimit
          ++ "."
      newParallelJobControl maxJobLimit
    Right server -> do
      let sem = serverClientSemaphore server
      info verbosity $
        "Created semaphore called "
          ++ semaphoreIdentifier (clientSemaphoreName sem)
          ++ " with "
          ++ show maxJobLimit
          ++ " slots."
      outqVar <- newTChanIO
      inqVar <- newTVarIO emptyJobQueue
      countVar <- newTVarIO 0
      void (forkIO (worker sem inqVar outqVar))
      return
        JobControl
          { spawnJobWithPriority = spawn inqVar countVar
          , collectJob = collect outqVar countVar
          , remainingJobs = remaining countVar
          , cancelJobs = cancel inqVar countVar
          , cleanupJobControl = destroyServerSemaphore server
          , jobControlSemaphore = Just (semaphoreIdentifier (clientSemaphoreName sem))
          }
  where
    worker :: ClientSemaphore -> TVar (JobQueue (IO a)) -> TChan (Either SomeException a) -> IO ()
    worker sem inqVar outqVar =
      forever $ do
        atomically $ check . not . nullJobQueue =<< readTVar inqVar
        -- Choose the job only once we hold a token: a job of higher priority
        -- may be spawned while we wait for it.
        --
        -- mask so that the fork happens atomically with the acquire.
        mask_ $ do
          -- waitOnSemaphore is interruptible under mask_
          tok <- waitOnSemaphore sem
          mjob <- atomically $ tryTakeJob inqVar
          case mjob of
            -- cancelJobs emptied the queue while we waited
            Nothing -> releaseSemaphoreToken tok
            Just job ->
              void $ forkIOWithUnmask $ \unmask -> do
                res <- try (unmask job) `finally` releaseSemaphoreToken tok
                atomically $ writeTChan outqVar res
        -- Try to give GHC enough time to compute the module graph and then
        -- request some additional capabilities if it can make use of them. The
        -- ideal situation is that we have 1 GHC process running which has taken
        -- all the capabilities in the semaphore, as this will reduce memory usage.
        --
        -- 0.25s is chosen by discussion between MP and SD on Mar 17th 2023 as a number
        -- which isn't too big and not too small but also, not scientifically.
        threadDelay 250000

    spawn :: TVar (JobQueue (IO a)) -> TVar Int -> JobPriority -> IO a -> IO ()
    spawn inqVar countVar prio job =
      atomically $ do
        modifyTVar' countVar (+ 1)
        modifyTVar' inqVar (pushJob prio job)

    collect :: TChan (Either SomeException a) -> TVar Int -> IO a
    collect outqVar countVar = do
      res <- atomically $ do
        modifyTVar' countVar (subtract 1)
        readTChan outqVar
      either throwIO return res

    remaining :: TVar Int -> IO Bool
    remaining countVar = fmap (/= 0) $ atomically $ readTVar countVar

    cancel :: TVar (JobQueue (IO a)) -> TVar Int -> IO ()
    cancel inqVar countVar =
      atomically $ do
        n <- dropJobs inqVar
        modifyTVar' countVar (subtract n)

-------------------------
-- Job limits and locks
--

data JobLimit = JobLimit QSem

newJobLimit :: Int -> IO JobLimit
newJobLimit n =
  fmap JobLimit (newQSem n)

withJobLimit :: JobLimit -> IO a -> IO a
withJobLimit (JobLimit sem) =
  bracket_ (waitQSem sem) (signalQSem sem)

newtype Lock = Lock (MVar ())

newLock :: IO Lock
newLock = Lock <$> newMVar ()

criticalSection :: Lock -> IO a -> IO a
criticalSection (Lock lck) act = bracket_ (takeMVar lck) (putMVar lck ()) act

--------------------------------------------------------------------------------
-- More high level utils
--------------------------------------------------------------------------------

newJobControlFromParStrat
  :: Verbosity
  -> Maybe Compiler
  -- ^ The compiler, used to determine whether Jsem is supported.
  -- When Nothing, Jsem is assumed to be unsupported.
  -> ParStratInstall
  -- ^ The parallel strategy
  -> Maybe Int
  -- ^ A cap on the number of jobs (e.g. to force a maximum of 2 concurrent downloads despite a -j8 parallel strategy)
  -> IO (JobControl IO a)
newJobControlFromParStrat verbosity mcompiler parStrat numJobsCap = case parStrat of
  Serial -> newSerialJobControl
  NumJobs n -> newParallelJobControl (capJobs (fromMaybe numberOfProcessors n))
  UseSem n ->
    case mcompiler of
      Just compiler
        | jsemSupported compiler
        , isJsemCompatible compiler ->
            newSemaphoreJobControl verbosity (capJobs n)
        | jsemSupported compiler ->
            do
              warn verbosity $
                "Semaphore version mismatch (cabal-install uses v"
                  ++ show (getSemaphoreProtocolVersion semaphoreVersion)
                  ++ ", but the selected GHC reports "
                  ++ maybe "no version (assumed v1)" (\v -> "v" ++ show v) (jsemVersion compiler)
                  ++ "); not using -jsem, GHC will be invoked without semaphore-based parallelism."
              newParallelJobControl (capJobs n)
        | otherwise ->
            do
              warn verbosity $
                "-jsem is not supported by the selected compiler; falling back to -j"
                  ++ show (capJobs n)
                  ++ "."
              newParallelJobControl (capJobs n)
      Nothing ->
        -- Don't warn in the Nothing case, as there isn't really a "selected" compiler.
        newParallelJobControl (capJobs n)
  where
    capJobs n = min (fromMaybe maxBound numJobsCap) n

-- | Check if the compiler's semaphore version is compatible with ours,
-- per 'versionsAreCompatible'. A compiler that doesn't report a
-- @"Semaphore version"@ field is treated as v1.
isJsemCompatible :: Compiler -> Bool
isJsemCompatible compiler =
  versionsAreCompatible (SemaphoreProtocolVersion v) semaphoreVersion
  where
    v = fromMaybe 1 (jsemVersion compiler)

withJobControl :: IO (JobControl IO a) -> (JobControl IO a -> IO b) -> IO b
withJobControl mkJC = bracket mkJC cleanupJobControl

-- | Concurrently execute actions on a list using the given JobControl.
-- The maximum number of concurrent jobs is tied to the JobControl instance.
-- The resulting list does /not/ preserve the original order!
mapConcurrentWithJobs :: JobControl IO b -> (a -> IO b) -> [a] -> IO [b]
mapConcurrentWithJobs jobControl f xs = do
  traverse_ (spawnJob jobControl . f) xs
  traverse (const $ collectJob jobControl) xs
