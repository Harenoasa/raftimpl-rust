impl < __Context > :: bincode :: Decode < __Context > for
AppendEntriesRpcResponse
{
    fn decode < __D : :: bincode :: de :: Decoder < Context = __Context > >
    (decoder : & mut __D) ->core :: result :: Result < Self, :: bincode ::
    error :: DecodeError >
    {
        core :: result :: Result ::
        Ok(Self
        {
            term_results : :: bincode :: Decode :: decode(decoder) ?, success
            : :: bincode :: Decode :: decode(decoder) ?,
        })
    }
} impl < '__de, __Context > :: bincode :: BorrowDecode < '__de, __Context >
for AppendEntriesRpcResponse
{
    fn borrow_decode < __D : :: bincode :: de :: BorrowDecoder < '__de,
    Context = __Context > > (decoder : & mut __D) ->core :: result :: Result <
    Self, :: bincode :: error :: DecodeError >
    {
        core :: result :: Result ::
        Ok(Self
        {
            term_results : :: bincode :: BorrowDecode ::< '_, __Context >::
            borrow_decode(decoder) ?, success : :: bincode :: BorrowDecode ::<
            '_, __Context >:: borrow_decode(decoder) ?,
        })
    }
}