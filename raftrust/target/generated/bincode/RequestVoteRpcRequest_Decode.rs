impl < __Context > :: bincode :: Decode < __Context > for
RequestVoteRpcRequest
{
    fn decode < __D : :: bincode :: de :: Decoder < Context = __Context > >
    (decoder : & mut __D) ->core :: result :: Result < Self, :: bincode ::
    error :: DecodeError >
    {
        core :: result :: Result ::
        Ok(Self
        {
            term_candidate : :: bincode :: Decode :: decode(decoder) ?,
            candidate_id : :: bincode :: Decode :: decode(decoder) ?,
            last_log_index : :: bincode :: Decode :: decode(decoder) ?,
            last_log_term : :: bincode :: Decode :: decode(decoder) ?,
        })
    }
} impl < '__de, __Context > :: bincode :: BorrowDecode < '__de, __Context >
for RequestVoteRpcRequest
{
    fn borrow_decode < __D : :: bincode :: de :: BorrowDecoder < '__de,
    Context = __Context > > (decoder : & mut __D) ->core :: result :: Result <
    Self, :: bincode :: error :: DecodeError >
    {
        core :: result :: Result ::
        Ok(Self
        {
            term_candidate : :: bincode :: BorrowDecode ::< '_, __Context >::
            borrow_decode(decoder) ?, candidate_id : :: bincode ::
            BorrowDecode ::< '_, __Context >:: borrow_decode(decoder) ?,
            last_log_index : :: bincode :: BorrowDecode ::< '_, __Context >::
            borrow_decode(decoder) ?, last_log_term : :: bincode ::
            BorrowDecode ::< '_, __Context >:: borrow_decode(decoder) ?,
        })
    }
}