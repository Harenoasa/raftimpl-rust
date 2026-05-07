impl :: bincode :: Encode for RequestVoteRpcRequest
{
    fn encode < __E : :: bincode :: enc :: Encoder >
    (& self, encoder : & mut __E) ->core :: result :: Result < (), :: bincode
    :: error :: EncodeError >
    {
        :: bincode :: Encode :: encode(&self.term_candidate, encoder) ?; ::
        bincode :: Encode :: encode(&self.candidate_id, encoder) ?; :: bincode
        :: Encode :: encode(&self.last_log_index, encoder) ?; :: bincode ::
        Encode :: encode(&self.last_log_term, encoder) ?; core :: result ::
        Result :: Ok(())
    }
}