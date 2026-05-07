impl :: bincode :: Encode for InstallSnapshotRpcRequest
{
    fn encode < __E : :: bincode :: enc :: Encoder >
    (& self, encoder : & mut __E) ->core :: result :: Result < (), :: bincode
    :: error :: EncodeError >
    {
        :: bincode :: Encode :: encode(&self.term_leader, encoder) ?; ::
        bincode :: Encode :: encode(&self.leader_id, encoder) ?; :: bincode ::
        Encode :: encode(&self.last_included_index, encoder) ?; :: bincode ::
        Encode :: encode(&self.last_included_term, encoder) ?; :: bincode ::
        Encode :: encode(&self.offset, encoder) ?; :: bincode :: Encode ::
        encode(&self.data, encoder) ?; :: bincode :: Encode ::
        encode(&self.done, encoder) ?; core :: result :: Result :: Ok(())
    }
}