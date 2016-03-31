-record(req, {
          id      :: {integer(), atom()} | integer(),
          request :: term(),
          bucket  :: binary()
         }).

-define(REQ(R), #req{request = R, bucket = ?BUCKET}).
-define(REQ(ID, R), #req{id = ID, request = R, bucket = ?BUCKET}).

