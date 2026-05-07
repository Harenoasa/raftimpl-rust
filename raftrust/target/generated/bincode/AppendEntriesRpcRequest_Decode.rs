impl < __Context > :: bincode :: Decode < __Context > for
AppendEntriesRpcRequest
{
    fn decode < __D : :: bincode :: de :: Decoder < Context = __Context > >
    (decoder : & mut __D) ->core :: result :: Result < Self, :: bincode ::
    error :: DecodeError >
    {
        core :: result :: Result ::
        Ok(Self
        {
            leader_term : :: bincode :: Decode :: decode(decoder) ?, leader_id
            : :: bincode :: Decode :: decode(decoder) ?, prev_log_index : ::
            bincode :: Decode :: decode(decoder) ?, prev_log_term : :: bincode
            :: Decode :: decode(decoder) ?, leader_commit : :: bincode ::
            Decode :: decode(decoder) ?, entries : :: bincode :: Decode ::
            decode(decoder) ?,
        })
    }
} impl < '__de, __Context > :: bincode :: BorrowDecode < '__de, __Context >
for AppendEntriesRpcRequest
{
    fn borrow_decode < __D : :: bincode :: de :: BorrowDecoder < '__de,
    Context = __Context > > (decoder : & mut __D) ->core :: result :: Result <
    Self, :: bincode :: error :: DecodeError >
    {
        core :: result :: Result ::
        Ok(Self
        {
            leader_term : :: bincode :: BorrowDecode ::< '_, __Context >::
            borrow_decode(decoder) ?, leader_id : :: bincode :: BorrowDecode
            ::< '_, __Context >:: borrow_decode(decoder) ?, prev_log_index :
            :: bincode :: BorrowDecode ::< '_, __Context >::
            borrow_decode(decoder) ?, prev_log_term : :: bincode ::
            BorrowDecode ::< '_, __Context >:: borrow_decode(decoder) ?,
            leader_commit : :: bincode :: BorrowDecode ::< '_, __Context >::
            borrow_decode(decoder) ?, entries : :: bincode :: BorrowDecode ::<
            '_, __Context >:: borrow_decode(decoder) ?,
        })
    }
}