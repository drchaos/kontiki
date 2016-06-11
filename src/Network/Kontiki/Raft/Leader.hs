{-# LANGUAGE OverloadedStrings,
             RecordWildCards,
             ScopedTypeVariables,
             MultiWayIf #-}
-----------------------------------------------------------------------------
-- |
-- Module      :  Network.Kontiki.Raft.Leader
-- Copyright   :  (c) 2013, Nicolas Trangez
-- License     :  BSD-like
--
-- Maintainer  :  ikke@nicolast.be
--
-- This module implements the behavior of a node in 
-- `Network.Kontiki.Types.MLeader' mode.
-----------------------------------------------------------------------------
module Network.Kontiki.Raft.Leader where

import Data.List (sortBy)

import qualified Data.Map as Map
import qualified Data.Set as Set

import Data.Maybe (listToMaybe)

import Data.ByteString.Char8 ()

import Control.Monad (when)

import Control.Lens hiding (Index)

import Network.Kontiki.Log
import Network.Kontiki.Types
import Network.Kontiki.Monad
import Network.Kontiki.Raft.Utils


-- | Handles `RequestVote'.
handleRequestVote :: (Functor m, Monad m)
                  => MessageHandler RequestVote a Leader m
handleRequestVote sender RequestVote{..} = do
    currentTerm <- use lCurrentTerm

    -- | TODO: Check that leader can't get not up-to-date log in the same
    -- term
    logS "Not granting vote"
    send sender $ RequestVoteResponse { rvrTerm = currentTerm
                                      , rvrVoteGranted = False
                                      }
    currentState

-- | Handle `RequestVoteResponse'.
handleRequestVoteResponse :: (Functor m, Monad m)
                          => MessageHandler RequestVoteResponse a Leader m
handleRequestVoteResponse sender RequestVoteResponse{..} =
    currentState

-- | Handles `AppendEntries'.
handleAppendEntries :: (Functor m, Monad m)
                    => MessageHandler (AppendEntries a) a Leader m
handleAppendEntries sender AppendEntries{..} =
    currentState

-- | Handles `AppendEntriesResponse'.
handleAppendEntriesResponse :: (Functor m, Monad m)
                            => MessageHandler AppendEntriesResponse a Leader m
handleAppendEntriesResponse sender AppendEntriesResponse{..} = do
    currentTerm <- use lCurrentTerm
    commitIndex <- use lCommitIndex

    if | aerTerm < currentTerm -> do
           logS "Ignoring old AppendEntriesResponse"
           currentState
       | not aerSuccess -> do
           lNextIndex %= Map.alter (\i -> Just $ min aerLastIndex $ maybe index0 prevIndex i) sender
           currentState
       | otherwise -> do
           lastIndices <- use lLastIndex
           let li = maybe index0 id (Map.lookup sender lastIndices)

           lLastIndex %= Map.insert sender aerLastIndex
           lNextIndex %= Map.insert sender aerLastIndex
           newQuorumIndex <- quorumIndex
           when (newQuorumIndex > commitIndex) $ do
               lCommitIndex .= newQuorumIndex
               setCommitIndex newQuorumIndex
           currentState

-- | Calculates current quorum `Index' from nodes' latest indices
quorumIndex :: (Functor m, Monad m)
            => TransitionT a LeaderState m Index
quorumIndex = do
    lastIndices <- Map.elems `fmap` use lLastIndex
    let sorted = sortBy (\a b -> compare b a) lastIndices
    quorum <- quorumSize
    return $ sorted !! (quorum - 1)

-- | Handles `ElectionTimeout'.
handleElectionTimeout :: (Functor m, Monad m)
                      => TimeoutHandler ElectionTimeout a Leader m
handleElectionTimeout = currentState

-- | Handles `HeartbeatTimeout'.
handleHeartbeatTimeout :: (Functor m, Monad m, MonadLog m a, GetNewNodeSet a)
                       => TimeoutHandler HeartbeatTimeout a Leader m
handleHeartbeatTimeout = do
    resetHeartbeatTimeout

    commitIndex <- use lCommitIndex

    lastEntry <- logLastEntry

    nodeId <- view configNodeId

    let lastIndex = maybe index0 eIndex lastEntry

    oldLastIndex <- (Map.! nodeId) `fmap` use lLastIndex
    lLastIndex %= Map.insert nodeId lastIndex 

    nsEntries <- filter hasNewNodeSet `fmap` getEntries [] oldLastIndex lastIndex

    when (length nsEntries > 1) $
        error "Too many new node sets."

    let nodeSetEntry = listToMaybe nsEntries

    nodes <- view configNodes

    case nodeSetEntry of
        Just ce  -> do
            let added    = Set.toList $ n Set.\\ nodes
                removed  = Set.toList $ nodes Set.\\ n
                (Just n) = getNodes ce
                lenAdded = length added
                lenRemoved = length removed
            updateNodeSet ce
            if | lenAdded == 1 && lenRemoved == 0 -> do
                    lLastIndex %= Map.insert (head added) index0
                    lNextIndex %= Map.insert (head added) (succIndex lastIndex)
               | lenAdded == 0 && lenRemoved == 1 -> do
                    lLastIndex %= Map.delete (head removed)
                    lNextIndex %= Map.delete (head removed)
               | lenAdded > 1   -> error "Only one node can be added at a time."
               | lenAdded /= 0 && lenRemoved /= 0 -> error "Nodes are added and removed."
               | lenRemoved > 1 -> error "Only one node can be removed at a time."
               | otherwise -> return ()
        Nothing -> return ()

    let otherNodes = filter (/= nodeId) (Set.toList $ maybe nodes id (maybe (Just nodes) getNodes nodeSetEntry))
    mapM_ (sendAppendEntries lastEntry commitIndex) otherNodes
    when (null otherNodes) $ do
        -- when no other nodes, no quorum
        let newQuorumIndex = lastIndex
        when (newQuorumIndex > commitIndex) $ do
            lCommitIndex .= newQuorumIndex
            setCommitIndex newQuorumIndex


    currentState

getEntries :: (Monad m, MonadLog m a)
           => [Entry a]
           -> Index
           -> Index ->
           m [Entry a]
getEntries acc fromI toI 
    | toI <= fromI = return acc
    | otherwise = do
        entry <- logEntry toI
        -- TODO Handle failure
        getEntries (maybe undefined id entry : acc) fromI (prevIndex toI)

-- | Sends `AppendEntries' to a particular `NodeId'.
sendAppendEntries :: (Monad m, MonadLog m a)
                  => Maybe (Entry a) -- ^ `Entry' to append
                  -> Index           -- ^ `Index' up to which the `Follower' should commit
                  -> NodeId          -- ^ `NodeId' to send to
                  -> TransitionT a LeaderState m ()
sendAppendEntries lastEntry commitIndex node = do
    currentTerm <- use lCurrentTerm

    nextIndices <- use lNextIndex

    let lastIndex = maybe index0 eIndex lastEntry
        lastTerm = maybe term0 eTerm lastEntry
        nextIndex = (Map.!) nextIndices node

    entries <- getEntries [] nextIndex lastIndex

    nodeId <- view configNodeId

    if null entries
        then send node AppendEntries { aeTerm = currentTerm
                                     , aeLeaderId = nodeId
                                     , aePrevLogIndex = lastIndex
                                     , aePrevLogTerm = lastTerm
                                     , aeEntries = []
                                     , aeCommitIndex = commitIndex
                                     }
        else do
            e <- logEntry (prevIndex $ eIndex $ head entries)
            send node AppendEntries { aeTerm = currentTerm
                                    , aeLeaderId = nodeId
                                    , aePrevLogIndex = maybe index0 eIndex e
                                    , aePrevLogTerm = maybe term0 eTerm e
                                    , aeEntries = entries
                                    , aeCommitIndex = commitIndex
                                    }

isSenderInConfig :: MessageFilter a
isSenderInConfig s m nodes = case m of 
    MAppendEntries {} -> True
    _                 -> s `Set.member` nodes

-- | `Handler' for `MLeader' mode.
handle :: (Functor m, Monad m, MonadLog m a, GetNewNodeSet a)
       => Handler a Leader m
handle = handleGeneric
            handleRequestVote
            handleRequestVoteResponse
            handleAppendEntries
            handleAppendEntriesResponse
            handleElectionTimeout
            handleHeartbeatTimeout
            isSenderInConfig

-- | Transitions into `MLeader' mode by broadcasting heartbeat `AppendEntries'
-- to all nodes and changing state to `LeaderState'. 
stepUp :: (Functor m, Monad m, MonadLog m a)
       => Term    -- ^ `Term' of the `Leader'
       -> Index   -- ^ commit `Index'
       -> TransitionT a f m SomeState
stepUp term commitIndex = do
    logS "Becoming leader"

    resetHeartbeatTimeout

    e <- logLastEntry
    let lastIndex = maybe index0 eIndex e
        lastTerm = maybe term0 eTerm e

    nodeId <- view configNodeId

    broadcast $ AppendEntries { aeTerm = term
                              , aeLeaderId = nodeId
                              , aePrevLogIndex = lastIndex
                              , aePrevLogTerm = lastTerm
                              , aeEntries = []
                              , aeCommitIndex = index0
                              }

    nodes <- view configNodes
    let ni = Map.fromList $ map (\n -> (n, succIndex lastIndex)) (Set.toList nodes)
        li = Map.fromList $ map (\n -> (n, index0)) (Set.toList nodes)

    return $ wrap $ LeaderState { _lCurrentTerm = term
                                , _lCommitIndex = commitIndex
                                , _lNextIndex = ni
                                , _lLastIndex = li
                                }
