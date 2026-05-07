impl :: bincode :: Encode for AppendEntriesRpcRequest
{
    fn encode < __E : :: bincode :: enc :: Encoder >
    (& self, encoder : & mut __E) ->core :: result :: Result < (), :: bincode
    :: error :: EncodeError >
    {
        :: bincode :: Encode :: encode(&self.leader_term, encoder) ?; ::
        bincode :: Encode :: encode(&self.leader_id, encoder) ?; :: bincode ::
        Encode :: encode(&self.prev_log_index, encoder) ?; :: bincode ::
        Encode :: encode(&self.prev_log_term, encoder) ?; :: bincode :: Encode
        :: encode(&self.leader_commit, encoder) ?; :: bincode :: Encode ::
        encode(&self.entries, encoder) ?; core :: result :: Result :: Ok(())
    }
}