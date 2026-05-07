impl :: bincode :: Encode for Entry
{
    fn encode < __E : :: bincode :: enc :: Encoder >
    (& self, encoder : & mut __E) ->core :: result :: Result < (), :: bincode
    :: error :: EncodeError >
    {
        :: bincode :: Encode :: encode(&self.term, encoder) ?; :: bincode ::
        Encode :: encode(&self.cmd, encoder) ?; core :: result :: Result ::
        Ok(())
    }
}