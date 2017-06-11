-module(memfd).

-export([ new/0
        , new/1
        , close/1
        , advise/4
        , allocate/3
        , truncate/1
        , truncate/2
        , read/2
        , write/2
        , position/2
        , pread/3
        , pwrite/3
        , seal/2
        , is_sealed/2
        , fd/1
        , owner/1
        , set_owner/2
        ]).

-on_load(init/0).

-include_lib("kernel/include/file.hrl").


-spec new() -> file:fd().

new() ->
    Id = erlang:unique_integer([positive]),
    Name = lists:flatten(io_lib:format("~p-~w", [self(), Id])),
    case create_nif(Name) of
        Data -> #file_descriptor{module=?MODULE, data=Data}
    end.


-spec new(FdBin) -> Fd when
      FdBin :: binary(),
      Fd  :: file:fd().

new(FdBin) ->
    case from_fd_nif(FdBin) of
        Data -> #file_descriptor{module=?MODULE, data=Data}
    end.


-spec close(Fd) -> ok | {error, Reason} when
      Fd     :: file:fd(),
      Reason :: file:posix().

close(#file_descriptor{data=Data}) ->
    close_nif(Data);

close(_) ->
    error(badarg).


-spec advise(Fd, Offset, Length, Advise) -> ok | {error, Reason} when
      Fd     :: file:fd(),
      Offset :: integer(),
      Length :: integer(),
      Advise :: file:posix_file_advise(),
      Reason :: file:posix().

advise(#file_descriptor{data=Data}, Offset, Length, Advise) ->
    advise_nif(Data, Offset, Length, Advise);

advise(_, _, _, _) ->
    error(badarg).


-spec allocate(Fd, Offset, Length) -> ok | {error, Reason} when
      Fd     :: file:fd(),
      Offset :: non_neg_integer(),
      Length :: non_neg_integer(),
      Reason :: file:posix().

allocate(#file_descriptor{data = Data}, Offset, Length) ->
    allocate_nif(Data, Offset, Length);

allocate(_,_,_) ->
    error(badarg).


-spec truncate(Fd) -> ok | {error, Reason} when
      Fd     :: file:fd(),
      Reason :: file:posix().

truncate(#file_descriptor{data=Data}) ->
    truncate_nif(Data);

truncate(_) ->
    error(badarg).


-spec truncate(Fd, Size) -> ok | {error, Reason} when
      Fd     :: file:fd(),
      Size   :: non_neg_integer(),
      Reason :: file:posix().

truncate(#file_descriptor{data=Data}, Size) ->
    truncate_nif(Data, Size);

truncate(_, _) ->
    error(badarg).


-spec read(Fd, Size) -> {ok, Data} | eof | {error, Reason} when
      Fd     :: file:fd(),
      Size   :: non_neg_integer(),
      Data   :: binary(),
      Reason :: file:posix().

read(#file_descriptor{data = Data}, Size) ->
    read_nif(Data, Size);

read(_, _) ->
    error(badarg).


-spec write(Fd, IoData) -> ok | {error, Reason} when
      Fd     :: file:fd(),
      IoData :: iodata(),
      Reason :: file:posix().

write(#file_descriptor{data = Data}, IoData) ->
    write_nif(Data, IoData);

write(_, _) ->
    error(badarg).


-spec position(Fd, Location) -> {ok, NewPosition} | {error, Reason} when
      Fd          :: file:fd(),
      Location    :: file:location(),
      NewPosition :: integer(),
      Reason      :: file:posix().

position(#file_descriptor{data = Data}, Location) ->
    position_nif(Data, normalize_location(Location));

position(_, _) ->
    error(badarg).


-spec pread(Fd, Location, Size) -> {ok, Data} | eof | {error, Reason} when
      Fd       :: file:fd(),
      Location :: non_neg_integer(),
      Size     :: non_neg_integer(),
      Data     :: binary(),
      Reason   :: file:posix().

pread(#file_descriptor{data = Data}, Offset, Size) when is_integer(Offset)->
    pread_nif(Data, Offset, Size);

pread(Fd, Location, Size) ->
    case position(Fd, Location) of
        {ok, _} -> read(Fd, Size);
        Error   -> Error
    end.


-spec pwrite(Fd, Location, Bytes) -> ok | {error, Reason} when
      Fd       :: file:fd(),
      Location :: file:location(),
      Bytes    :: iodata(),
      Reason   :: file:posix().

pwrite(#file_descriptor{data = Data}, Offset, Bytes) when is_integer(Offset) ->
    pwrite_nif(Data, Offset, Bytes);

pwrite(Fd, Location, Bytes) ->
    case position(Fd, Location) of
        {ok, _} -> write(Fd, Bytes);
        Error   -> Error
    end.


-spec seal(Fd, Seal) -> ok | {error, Reason} when
      Fd   :: file:fd(),
      Seal :: seal | shrink | grow | write,
      Reason   :: file:posix().

seal(#file_descriptor{data = Data}, Seal) ->
    seal_nif(Data, Seal).


-spec is_sealed(Fd, Seal) -> true | false when
      Fd   :: file:fd(),
      Seal :: seal | shrink | grow | write.

is_sealed(#file_descriptor{data = Data}, Seal) ->
    is_sealed_nif(Data, Seal).


-spec fd(Fd) -> binary() when Fd :: file:fd().

fd(#file_descriptor{data=Data}) ->
    to_fd_nif(Data).


-spec owner(Fd) -> Pid when
      Fd   :: file:fd(),
      Pid  :: pid().

owner(#file_descriptor{data = Data}) ->
    owner_nif(Data).


-spec set_owner(Fd, Pid) -> ok  when
      Fd  :: file:fd(),
      Pid :: pid().

set_owner(#file_descriptor{data = Data}, Pid) ->
    set_owner_nif(Data, Pid).


normalize_location(Location) ->
    case Location of
        {_, _} = Pos             -> Pos;
        'bof'                    -> {'bof', 0};
        'cur'                    -> {'cur', 0};
        'eof'                    -> {'eof', 0};
        Pos when is_integer(Pos) -> {'bof', Pos};
        _                        -> error(badarg)
    end.


init() ->
    erlang:load_nif(filename:join(code:priv_dir(memfd), "memfd"), 0).


%% NIFs

create_nif(Name) ->
    erlang:nif_error(not_loaded, [Name]).

from_fd_nif(FdBin) ->
    erlang:nif_error(not_loaded, [FdBin]).

close_nif(Data) ->
    erlang:nif_error(not_loaded, [Data]).

to_fd_nif(Data) ->
    erlang:nif_error(not_loaded, [Data]).

advise_nif(Data, Offset, Length, Advise) ->
    erlang:nif_error(not_loaded, [Data, Offset, Length, Advise]).

allocate_nif(Data, Offset, Length) ->
    erlang:nif_error(not_loaded, [Data, Offset, Length]).

truncate_nif(Data) ->
    erlang:nif_error(not_loaded, [Data]).

truncate_nif(Data, Size) ->
    erlang:nif_error(not_loaded, [Data, Size]).

read_nif(Data, Size) ->
    erlang:nif_error(not_loaded, [Data, Size]).

write_nif(Data, IoData) ->
    erlang:nif_error(not_loaded, [Data, IoData]).

position_nif(Data, At) ->
    erlang:nif_error(not_loaded, [Data, At]).

pread_nif(Data, At, Size) ->
    erlang:nif_error(not_loaded, [Data, At, Size]).

pwrite_nif(Data, At, Bytes) ->
    erlang:nif_error(not_loaded, [Data, At, Bytes]).

seal_nif(Data, Seal) ->
    erlang:nif_error(not_loaded, [Data, Seal]).

is_sealed_nif(Data, Seal) ->
    erlang:nif_error(not_loaded, [Data, Seal]).

owner_nif(Data) ->
    erlang:nif_error(not_loaded, [Data]).

set_owner_nif(Data, Pid) ->
    erlang:nif_error(not_loaded, [Data, Pid]).
