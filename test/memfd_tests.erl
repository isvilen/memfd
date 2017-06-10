-module(memfd_tests).
-include_lib("eunit/include/eunit.hrl").

create_close_test() ->
    Fd = memfd:create(),
    ?assert(is_record(Fd, file_descriptor, 3)),
    ?assertEqual(ok, file:close(Fd)).


advise_test() ->
    Fd = memfd:create(),
    ?assertEqual(ok, file:advise(Fd, 0, 10000, normal)),
    ?assertEqual(ok, file:advise(Fd, 0, 10000, sequential)),
    ?assertEqual(ok, file:advise(Fd, 0, 10000, random)),
    ?assertEqual(ok, file:advise(Fd, 0, 10000, no_reuse)),
    ?assertEqual(ok, file:advise(Fd, 0, 10000, will_need)),
    ?assertEqual(ok, file:advise(Fd, 0, 10000, dont_need)).


allocate_test() ->
    Fd = memfd:create(),
    ?assertEqual(eof, file:read(Fd, 10)),
    ?assertEqual(ok, file:allocate(Fd, 0, 10)),
    ?assertEqual({ok, <<0:80>>}, file:read(Fd,10)),
    ?assertEqual(eof, file:read(Fd, 10)),
    ?assertEqual(ok, file:allocate(Fd, 10, 10)),
    ?assertEqual({ok, <<0:80>>}, file:read(Fd,10)).


read_write_test() ->
    Fd = memfd:create(),
    ?assertEqual(ok, file:write(Fd, <<1,2,3,4,5>>)),
    ?assertEqual({ok, 0}, file:position(Fd, bof)),
    ?assertEqual({ok, <<1,2,3,4,5>>}, file:read(Fd,5)).


pread_pwrite_test() ->
    Fd = memfd:create(),
    ?assertEqual(ok, file:pwrite(Fd, 4, <<1,2,3,4>>)),
    ?assertEqual({ok, <<0,0,1,2,3,4>>}, file:pread(Fd, 2, 6)).


file_truncate_test() ->
    Fd = memfd:create(),
    ?assertEqual(ok, file:write(Fd, <<1,2,3,4,5,6,7,8,9,10>>)),
    ?assertEqual({ok, 5}, file:position(Fd, 5)),
    ?assertEqual(ok, file:truncate(Fd)),
    ?assertEqual({ok, 0}, file:position(Fd, bof)),
    ?assertEqual({ok, <<1,2,3,4,5>>}, file:read(Fd, 10)).


memfd_truncate_test() ->
    Fd = memfd:create(),
    ?assertEqual(ok, file:write(Fd, <<1,2,3,4,5,6,7,8,9,10>>)),
    ?assertEqual(ok, memfd:truncate(Fd, 5)),
    ?assertEqual({ok, 0}, file:position(Fd, bof)),
    ?assertEqual({ok, <<1,2,3,4,5>>}, file:read(Fd, 10)).


from_to_binary_test() ->
    Fd1 = memfd:create(),
    FdBin = memfd:fd_to_binary(Fd1),
    Fd2 = memfd:fd_from_binary(FdBin),
    ?assertEqual(ok, file:write(Fd1, <<1,2,3,4,5>>)),
    ?assertEqual({ok, 0}, file:position(Fd2, bof)),
    ?assertEqual({ok, <<1,2,3,4,5>>}, file:read(Fd2,5)),
    ?assertEqual(ok, file:close(Fd1)),
    ?assertEqual(ok, file:close(Fd2)).
