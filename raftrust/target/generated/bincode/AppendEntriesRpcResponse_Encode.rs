impl :: bincode :: Encode for AppendEntriesRpcResponse
{
    fn encode < __E : :: bincode :: enc :: Encoder >
    (& self, encoder : & mut __E) ->core :: result :: Result < (), :: bincode
    :: error :: EncodeError >
    {
        :: bincode :: Encode :: encode(&self.term_results, encoder) ?; ::
        bincode :: Encode :: encode(&self.success, encoder) ?; core :: result
        :: Result :: Ok(())
    }
}