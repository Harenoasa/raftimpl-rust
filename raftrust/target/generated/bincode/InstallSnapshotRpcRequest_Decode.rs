impl < __Context > :: bincode :: Decode < __Context > for
InstallSnapshotRpcRequest
{
    fn decode < __D : :: bincode :: de :: Decoder < Context = __Context > >
    (decoder : & mut __D) ->core :: result :: Result < Self, :: bincode ::
    error :: DecodeError >
    {
        core :: result :: Result ::
        Ok(Self
        {
            term_leader : :: bincode :: Decode :: decode(decoder) ?, leader_id
            : :: bincode :: Decode :: decode(decoder) ?, last_included_index :
            :: bincode :: Decode :: decode(decoder) ?, last_included_term : ::
            bincode :: Decode :: decode(decoder) ?, offset : :: bincode ::
            Decode :: decode(decoder) ?, data : :: bincode :: Decode ::
            decode(decoder) ?, done : :: bincode :: Decode :: decode(decoder)
            ?,
        })
    }
} impl < '__de, __Context > :: bincode :: BorrowDecode < '__de, __Context >
for InstallSnapshotRpcRequest
{
    fn borrow_decode < __D : :: bincode :: de :: BorrowDecoder < '__de,
    Context = __Context > > (decoder : & mut __D) ->core :: result :: Result <
    Self, :: bincode :: error :: DecodeError >
    {
        core :: result :: Result ::
        Ok(Self
        {
            term_leader : :: bincode :: BorrowDecode ::< '_, __Context >::
            borrow_decode(decoder) ?, leader_id : :: bincode :: BorrowDecode
            ::< '_, __Context >:: borrow_decode(decoder) ?,
            last_included_index : :: bincode :: BorrowDecode ::< '_, __Context
            >:: borrow_decode(decoder) ?, last_included_term : :: bincode ::
            BorrowDecode ::< '_, __Context >:: borrow_decode(decoder) ?,
            offset : :: bincode :: BorrowDecode ::< '_, __Context >::
            borrow_decode(decoder) ?, data : :: bincode :: BorrowDecode ::<
            '_, __Context >:: borrow_decode(decoder) ?, done : :: bincode ::
            BorrowDecode ::< '_, __Context >:: borrow_decode(decoder) ?,
        })
    }
}