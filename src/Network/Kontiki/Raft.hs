{-# LANGUAGE GADTs,
             RankNTypes,
             OverloadedStrings #-}
-----------------------------------------------------------------------------
-- |
-- Module      :  Network.Kontiki.Raft
-- Copyright   :  (c) 2013, Nicolas Trangez
-- License     :  BSD-like
--
-- Maintainer  :  ikke@nicolast.be
--
-- Model only implementation of the Raft protocol 
-- <https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf>.
--
-- This module provides the main API for using the model implementation of
-- the Raft protocol. Importing this module should be sufficient to implement
-- a driver to the `Network.Kontiki.Types.Command's emitted from the model. 
-----------------------------------------------------------------------------
module Network.Kontiki.Raft (
      module Network.Kontiki.Log
    , module Network.Kontiki.Types
    , module Network.Kontiki.Monad
    , handle
    , initialState
    , restore
    ) where

import Control.Lens

import Network.Kontiki.Log
import Network.Kontiki.Types
import Network.Kontiki.Monad

import qualified Network.Kontiki.Raft.Follower as Follower
import qualified Network.Kontiki.Raft.Candidate as Candidate
import qualified Network.Kontiki.Raft.Leader as Leader
import Network.Kontiki.Raft.Utils

import Data.Maybe (fromJust)
import Control.Monad.Writer (runWriterT)
import Control.Monad.Reader (runReader)

-- | Main handler function which, given the `config' of the cluster 
-- and `state' of the node, runs the Raft protocol and  
-- returns the new state of the node and a list of commands that 
-- should be executed by the driver.
handle :: (Functor m, Monad m, MonadLog m a, GetNewNodeSet a) 
       => Config                        -- ^ configuration of the cluster 
       -> SomeState                     -- ^ current state of the node
       -> Event a                       -- ^ incoming event
       -> m (SomeState, [Command a])    -- ^ new state and list of commands
handle config state event = do
    let (state' , cmd) = updateTerm config state event
    (state'', cmd') <- case state' of
                            WrapState(Follower s') -> select `fmap` runTransitionT (Follower.handle event) config s'
                            WrapState(Candidate s') -> select `fmap` runTransitionT (Candidate.handle event) config s'
                            WrapState(Leader s') -> select `fmap` runTransitionT (Leader.handle event) config s'
    return (state'', cmd ++ cmd')
  where
    -- | Drops the middle value from a three-tuple
    select :: (a, b, c) -> (a, c)
    select (a, _, c) = (a, c)

updateTerm :: Config
           -> SomeState
           -> Event a
           -> (SomeState, [Command a])
updateTerm config state event =
    if maybe False (> currentTerm state) newTerm
        then (`runReader` config) $ runWriterT $ do
            logS "Update current term"
            stepDown (fromJust newTerm) (currentIndex state)
        else (state,[])
  where
    newTerm = messageTerm event

    messageTerm :: Event a -> Maybe Term
    messageTerm (EMessage _ (MRequestVote m)) = Just $ rvTerm m
    messageTerm (EMessage _ (MRequestVoteResponse m)) = Just $ rvrTerm m
    messageTerm (EMessage _ (MAppendEntries m)) = Just $ aeTerm m
    messageTerm (EMessage _ (MAppendEntriesResponse m)) = Just $ aerTerm m
    messageTerm EElectionTimeout = Nothing
    messageTerm EHeartbeatTimeout = Nothing

    currentTerm :: SomeState -> Term
    currentTerm (WrapState (Follower s)) = _fCurrentTerm s
    currentTerm (WrapState (Leader s)) = _lCurrentTerm s
    currentTerm (WrapState (Candidate s)) = _cCurrentTerm s

    currentIndex :: SomeState -> Network.Kontiki.Types.Index
    currentIndex (WrapState (Follower s)) = _fCommitIndex s
    currentIndex (WrapState (Leader s)) = _lCommitIndex s
    currentIndex (WrapState (Candidate s)) = _cCommitIndex s

-- | Initial state of all nodes.
initialState :: SomeState
initialState = wrap FollowerState { _fCurrentTerm = term0
                                  , _fCommitIndex = index0
                                  , _fVotedFor = Nothing
                                  }

-- | Restores the node to initial (`Follower') mode
-- and resets the election timeout. This function is useful
-- in order to kickstart the protocol by issuing the
-- `CResetElectionTimeout' command.
--
-- If the node is in either `Leader' or `Candidate' mode,
-- their state will be changed to having voted for itself.
--
-- In all modes, the current term will be kept.
restore :: Config                   -- ^ configuration of the cluster 
        -> SomeState                -- ^ current state of the node 
        -> (SomeState, [Command a]) -- ^ new state and list of commands
restore cfg s = case s of
    WrapState(Follower _) -> (s, commands)
    WrapState(Candidate s') -> (toFollower (s' ^. cCurrentTerm) (s' ^. cCommitIndex), commands)
    WrapState(Leader s') -> (toFollower (s' ^. lCurrentTerm) (s' ^. lCommitIndex), commands)
  where
    toFollower t i = wrap FollowerState { _fCurrentTerm = t
                                        , _fCommitIndex = i
                                        , _fVotedFor = Just nodeId
                                        }
    nodeId = cfg ^. configNodeId
    et = cfg ^. configElectionTimeout
    commands :: forall a. [Command a]
    commands = [CResetElectionTimeout et (2 * et)]
