-module(memfd_tests).
-include_lib("eunit/include/eunit.hrl").

create_close_test() ->
    Fd = memfd:new(),
    ?assert(is_record(Fd, file_descriptor, 3)),
    ?assertEqual(ok, file:close(Fd)).


advise_test() ->
    Fd = memfd:new(),
    ?assertEqual(ok, file:advise(Fd, 0, 10000, normal)),
    ?assertEqual(ok, file:advise(Fd, 0, 10000, sequential)),
    ?assertEqual(ok, file:advise(Fd, 0, 10000, random)),
    ?assertEqual(ok, file:advise(Fd, 0, 10000, no_reuse)),
    ?assertEqual(ok, file:advise(Fd, 0, 10000, will_need)),
    ?assertEqual(ok, file:advise(Fd, 0, 10000, dont_need)).


allocate_test() ->
    Fd = memfd:new(),
    ?assertEqual(eof, file:read(Fd, 10)),
    ?assertEqual(ok, file:allocate(Fd, 0, 10)),
    ?assertEqual({ok, <<0:80>>}, file:read(Fd,10)),
    ?assertEqual(eof, file:read(Fd, 10)),
    ?assertEqual(ok, file:allocate(Fd, 10, 10)),
    ?assertEqual({ok, <<0:80>>}, file:read(Fd,10)).


read_write_test() ->
    Fd = memfd:new(),
    ?assertEqual(ok, file:write(Fd, <<1,2,3,4,5>>)),
    ?assertEqual({ok, 0}, file:position(Fd, bof)),
    ?assertEqual({ok, <<1,2,3,4,5>>}, file:read(Fd,5)).


pread_pwrite_test() ->
    Fd = memfd:new(),
    ?assertEqual(ok, file:pwrite(Fd, 4, <<1,2,3,4>>)),
    ?assertEqual({ok, <<0,0,1,2,3,4>>}, file:pread(Fd, 2, 6)).


file_truncate_test() ->
    Fd = memfd:new(),
    ?assertEqual(ok, file:write(Fd, <<1,2,3,4,5,6,7,8,9,10>>)),
    ?assertEqual({ok, 5}, file:position(Fd, 5)),
    ?assertEqual(ok, file:truncate(Fd)),
    ?assertEqual({ok, 0}, file:position(Fd, bof)),
    ?assertEqual({ok, <<1,2,3,4,5>>}, file:read(Fd, 10)).


memfd_truncate_test() ->
    Fd = memfd:new(),
    ?assertEqual(ok, file:write(Fd, <<1,2,3,4,5,6,7,8,9,10>>)),
    ?assertEqual(ok, memfd:truncate(Fd, 5)),
    ?assertEqual({ok, 0}, file:position(Fd, bof)),
    ?assertEqual({ok, <<1,2,3,4,5>>}, file:read(Fd, 10)).


seal_test_() -> {foreach, fun memfd:new/0, [
  {with, [fun (Fd) ->
              ?assert(not memfd:is_sealed(Fd, seal)),
              ?assertEqual(ok, memfd:seal(Fd, seal)),
              ?assert(memfd:is_sealed(Fd, seal)),
              ?assertEqual({error, eperm}, memfd:seal(Fd, shrink))
          end]}

 ,{with, [fun (Fd) ->
              ?assert(not memfd:is_sealed(Fd, shrink)),
              ?assertEqual(ok, memfd:write(Fd, <<1,2,3,4>>)),
              ?assertEqual(ok, memfd:seal(Fd, shrink)),
              ?assert(memfd:is_sealed(Fd, shrink)),
              ?assertEqual({error, eperm}, memfd:truncate(Fd, 0)),
              ?assertEqual(ok, memfd:truncate(Fd, 8))
          end]}

 ,{with, [fun (Fd) ->
              ?assert(not memfd:is_sealed(Fd, grow)),
              ?assertEqual(ok, memfd:write(Fd, <<1,2,3,4>>)),
              ?assertEqual(ok, memfd:seal(Fd, grow)),
              ?assert(memfd:is_sealed(Fd, grow)),
              ?assertEqual({error, eperm}, memfd:truncate(Fd, 8)),
              ?assertEqual(ok, memfd:truncate(Fd, 0))
          end]}

 ,{with, [fun (Fd) ->
              ?assert(not memfd:is_sealed(Fd, write)),
              ?assertEqual(ok, memfd:write(Fd, <<1,2,3,4>>)),
              ?assertEqual(ok, memfd:seal(Fd, write)),
              ?assert(memfd:is_sealed(Fd, write)),
              ?assertEqual({error, eperm}, memfd:write(Fd, <<1,2,3,4>>))
          end]}
 ]}.


owner_test() ->
    Fd = memfd:new(),
    ?assertEqual(self(), memfd:owner(Fd)),

    Pid = spawn_link(fun Loop() ->
                       receive
                           {step1, Self} ->
                               Self ! memfd:set_owner(Fd, self()), Loop();
                           {step2, Self} ->
                               Self ! memfd:close(Fd), Loop();
                           {step3, Self} ->
                               Self ! memfd:close(Fd)
                       end
                     end),

    Pid ! {step1, self()},
    ?assertEqual({error, eperm}, receive X -> X end),

    Pid ! {step2, self()},
    ?assertEqual({error, eperm}, receive X -> X end),

    ?assertEqual(ok, memfd:set_owner(Fd, Pid)),
    ?assertEqual(Pid, memfd:owner(Fd)),

    Pid ! {step3, self()},
    ?assertEqual(ok, receive X -> X end).


from_to_binary_fd_test() ->
    Fd1 = memfd:new(),
    FdBin = memfd:fd(Fd1),
    Fd2 = memfd:new(FdBin),
    ?assertEqual(ok, file:write(Fd1, <<1,2,3,4,5>>)),
    ?assertEqual({ok, 0}, file:position(Fd2, bof)),
    ?assertEqual({ok, <<1,2,3,4,5>>}, file:read(Fd2,5)),
    ?assertEqual(ok, file:close(Fd1)),
    ?assertEqual(ok, file:close(Fd2)).


to_binary_test() ->
    Fd = memfd:new(),
    ?assertEqual(ok, file:write(Fd, <<1,2,3,4,5>>)),
    ?assertEqual({error, eperm}, memfd:to_binary(Fd)),
    ?assertEqual(ok, memfd:seal(Fd, shrink)),
    ?assertEqual({error, eperm}, memfd:to_binary(Fd)),
    ?assertEqual(ok, memfd:seal(Fd, write)),
    ?assertEqual({ok, <<1,2,3,4,5>>}, memfd:to_binary(Fd)).
