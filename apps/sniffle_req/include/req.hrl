-record(req, {
          id      :: {integer(), atom()} | integer() | undefined,
          request :: term(),
          bucket  :: binary() | undefined
         }).

-define(REQ(R), #req{request = R, bucket = ?BUCKET}).
-define(REQ(ID, R), #req{id = ID, request = R, bucket = ?BUCKET}).
