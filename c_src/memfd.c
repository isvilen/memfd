#define _GNU_SOURCE
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/syscall.h>

#include <alloca.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>

#include <linux/memfd.h>

#include <stdbool.h>

#include <erl_nif.h>
#include <erl_driver.h>

#if !defined(F_ADD_SEALS)
#define F_ADD_SEALS 1024 + 9
#endif

#if !defined(F_GET_SEALS)
#define F_GET_SEALS 1024 + 10
#endif

#if !defined(F_SEAL_SEAL)
#define F_SEAL_SEAL 0x0001
#endif

#if !defined(F_SEAL_SHRINK)
#define F_SEAL_SHRINK 0x0002
#endif


static ErlNifResourceType* memfd_type;
static ERL_NIF_TERM atom_ok;
static ERL_NIF_TERM atom_error;
static ERL_NIF_TERM atom_true;
static ERL_NIF_TERM atom_false;
static ERL_NIF_TERM atom_bof;
static ERL_NIF_TERM atom_cur;
static ERL_NIF_TERM atom_eof;
static ERL_NIF_TERM atom_normal;
static ERL_NIF_TERM atom_sequential;
static ERL_NIF_TERM atom_random;
static ERL_NIF_TERM atom_no_reuse;
static ERL_NIF_TERM atom_will_need;
static ERL_NIF_TERM atom_dont_need;


typedef struct {
    ErlNifRWLock *lock;
    ErlNifMonitor monitor;
    ErlNifPid owner;
    int fd;
    bool closed;
} memfd;


static ERL_NIF_TERM allocate_memfd(ErlNifEnv* env, int fd);
static bool is_owned(ErlNifEnv* env, memfd* mfd);
static ERL_NIF_TERM errno_error(ErlNifEnv* env, int error);
static ERL_NIF_TERM raise_errno_exception(ErlNifEnv* env, int error);


static ERL_NIF_TERM
create_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    unsigned int size;

    if (!enif_get_list_length(env, argv[0], &size))
        return enif_make_badarg(env);

    char *name = alloca(size+1);

    if (enif_get_string(env, argv[0], name, size+1, ERL_NIF_LATIN1) < 1)
        return enif_make_badarg(env);

    int fd = syscall(__NR_memfd_create, name, MFD_CLOEXEC | MFD_ALLOW_SEALING);
    if (fd < 0) return raise_errno_exception(env, errno);

    return allocate_memfd(env, fd);
}


static ERL_NIF_TERM
close_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    void *ptr;
    if (!enif_get_resource(env, argv[0], memfd_type, &ptr))
        return enif_make_badarg(env);

    memfd *mfd = (memfd *) ptr;

    enif_rwlock_rwlock(mfd->lock);

    if (mfd->closed) {
        enif_rwlock_rwunlock(mfd->lock);
        return errno_error(env, EBADF);
    }

    if (!is_owned(env, mfd)) {
        enif_rwlock_rwunlock(mfd->lock);
        return errno_error(env, EPERM);
    }

    mfd->closed = true;

    if (close(mfd->fd) < 0) {
        enif_rwlock_rwunlock(mfd->lock);
        return errno_error(env, errno);
    }

    enif_rwlock_rwunlock(mfd->lock);

    return atom_ok;
}


static ERL_NIF_TERM
advise_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    void *ptr;
    if (!enif_get_resource(env, argv[0], memfd_type, &ptr))
        return enif_make_badarg(env);

    unsigned int tmp;
    if (!enif_get_uint(env, argv[1], &tmp)) return enif_make_badarg(env);
    off_t offset = tmp;

    if (!enif_get_uint(env, argv[2], &tmp)) return enif_make_badarg(env);
    off_t len = tmp;

    int advice = 0;
    if (enif_compare(argv[3], atom_normal) == 0)
        advice = POSIX_FADV_NORMAL;
    else if (enif_compare(argv[3], atom_sequential) == 0)
        advice = POSIX_FADV_SEQUENTIAL;
    else if (enif_compare(argv[3], atom_random) == 0)
        advice = POSIX_FADV_RANDOM;
    else if (enif_compare(argv[3], atom_no_reuse) == 0)
        advice = POSIX_FADV_NOREUSE;
    else if (enif_compare(argv[3], atom_will_need) == 0)
        advice = POSIX_FADV_WILLNEED;
    else if (enif_compare(argv[3], atom_dont_need) == 0)
        advice = POSIX_FADV_DONTNEED;
    else return enif_make_badarg(env);

    memfd *mfd = (memfd *) ptr;

    enif_rwlock_rlock(mfd->lock);

    if (mfd->closed) {
        enif_rwlock_runlock(mfd->lock);
        return errno_error(env, EBADF);
    }

    int rc = posix_fadvise(mfd->fd, offset, len, advice);

    enif_rwlock_runlock(mfd->lock);

    if (rc != 0) return errno_error(env, rc);

    return atom_ok;
}


static ERL_NIF_TERM
allocate_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    void *ptr;
    if (!enif_get_resource(env, argv[0], memfd_type, &ptr))
        return enif_make_badarg(env);

    unsigned int tmp;
    if (!enif_get_uint(env, argv[1], &tmp)) return enif_make_badarg(env);
    off_t offset = tmp;

    if (!enif_get_uint(env, argv[2], &tmp)) return enif_make_badarg(env);
    off_t len = tmp;

    memfd *mfd = (memfd *) ptr;

    enif_rwlock_rwlock(mfd->lock);

    if (mfd->closed) {
        enif_rwlock_rwunlock(mfd->lock);
        return errno_error(env, EBADF);
    }

    int rc = posix_fallocate(mfd->fd, offset, len);

    enif_rwlock_rwunlock(mfd->lock);

    if (rc != 0) return errno_error(env, rc);

    return atom_ok;
}


static ERL_NIF_TERM
truncate_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    void *ptr;
    if (!enif_get_resource(env, argv[0], memfd_type, &ptr))
        return enif_make_badarg(env);

    off_t length = (off_t) -1;

    if (argc > 1) {
        unsigned int tmp;
        if (!enif_get_uint(env, argv[1], &tmp)) return enif_make_badarg(env);
        length = tmp;
    }

    memfd *mfd = (memfd *) ptr;

    enif_rwlock_rwlock(mfd->lock);

    if (mfd->closed) {
        enif_rwlock_rwunlock(mfd->lock);
        return errno_error(env, EBADF);
    }

    if (length == (off_t) -1) {
        length = lseek(mfd->fd, 0, SEEK_CUR);

        if (length == (off_t) -1) {
            enif_rwlock_rwunlock(mfd->lock);
            return errno_error(env, errno);
        }
    }

    int rc = ftruncate(mfd->fd, length);

    enif_rwlock_rwunlock(mfd->lock);

    if (rc < 0) return errno_error(env, errno);

    return atom_ok;
}


static ERL_NIF_TERM
read_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    void *ptr;
    if (!enif_get_resource(env, argv[0], memfd_type, &ptr))
        return enif_make_badarg(env);

    unsigned int size;
    if (!enif_get_uint(env, argv[1], &size)) return enif_make_badarg(env);

    ErlNifBinary buf;
    if (!enif_alloc_binary(size, &buf)) return errno_error(env, ENOMEM);

    memfd *mfd = (memfd *) ptr;

    enif_rwlock_rlock(mfd->lock);

    if (mfd->closed) {
        enif_rwlock_runlock(mfd->lock);
        enif_release_binary(&buf);
        return errno_error(env, EBADF);
    }

    ssize_t rc = read(mfd->fd, buf.data, buf.size);

    enif_rwlock_runlock(mfd->lock);

    if (rc < 0) {
        enif_release_binary(&buf);
        return errno_error(env, errno);
    }

    if (rc == 0) {
        enif_release_binary(&buf);
        return atom_eof;
    }

    if (!enif_realloc_binary(&buf, rc)) {
        enif_release_binary(&buf);
        return errno_error(env, ENOMEM);
    }

    ERL_NIF_TERM result = enif_make_binary(env, &buf);
    return enif_make_tuple2(env, atom_ok, result);
}


static ERL_NIF_TERM
write_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    void *ptr;
    if (!enif_get_resource(env, argv[0], memfd_type, &ptr))
        return enif_make_badarg(env);

    ErlNifBinary bin;
    if (!enif_inspect_binary(env, argv[1], &bin)) {
        if (!enif_inspect_iolist_as_binary(env, argv[1], &bin)) {
            return enif_make_badarg(env);
        }
    }

    memfd *mfd = (memfd *) ptr;

    enif_rwlock_rwlock(mfd->lock);

    if (mfd->closed) {
        enif_rwlock_rwunlock(mfd->lock);
        return errno_error(env, EBADF);
    }

    ssize_t rc = write(mfd->fd, bin.data, bin.size);

    enif_rwlock_rwunlock(mfd->lock);

    if (rc < 0) return errno_error(env, errno);

    if (rc != bin.size) return errno_error(env, ENOMEM);

    return atom_ok;
}


static ERL_NIF_TERM
position_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    void *ptr;
    if (!enif_get_resource(env, argv[0], memfd_type, &ptr))
        return enif_make_badarg(env);

    int arity;
    const ERL_NIF_TERM  *elems;

    if (!enif_get_tuple(env, argv[1], &arity, &elems) || arity != 2)
        return enif_make_badarg(env);

    int whence;
    if (enif_compare(elems[0], atom_bof) == 0)
        whence = SEEK_SET;
    else if (enif_compare(elems[0], atom_cur) == 0)
        whence = SEEK_CUR;
    else if (enif_compare(elems[0], atom_eof) == 0)
        whence = SEEK_END;
    else
        return enif_make_badarg(env);

    long offset;
    if (!enif_get_long(env, elems[1], &offset))
        return enif_make_badarg(env);

    memfd *mfd = (memfd *) ptr;

    enif_rwlock_rlock(mfd->lock);

    if (mfd->closed) {
        enif_rwlock_runlock(mfd->lock);
        return errno_error(env, EBADF);
    }

    off_t np = lseek(mfd->fd, offset, whence);

    enif_rwlock_runlock(mfd->lock);

    if (np == (off_t) -1) return errno_error(env, errno);

    ERL_NIF_TERM result = enif_make_uint(env, np);
    return enif_make_tuple2(env, atom_ok, result);
}


static ERL_NIF_TERM
pread_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    void *ptr;
    if (!enif_get_resource(env, argv[0], memfd_type, &ptr))
        return enif_make_badarg(env);

    unsigned int tmp;
    if (!enif_get_uint(env, argv[1], &tmp)) return enif_make_badarg(env);
    off_t offset = tmp;

    unsigned int size;
    if (!enif_get_uint(env, argv[2], &size)) return enif_make_badarg(env);

    ErlNifBinary buf;
    if (!enif_alloc_binary(size, &buf)) return errno_error(env, ENOMEM);

    memfd *mfd = (memfd *) ptr;

    enif_rwlock_rlock(mfd->lock);

    if (mfd->closed) {
        enif_rwlock_runlock(mfd->lock);
        enif_release_binary(&buf);
        return errno_error(env, EBADF);
    }

    ssize_t rc = pread(mfd->fd, buf.data, buf.size, offset);

    enif_rwlock_runlock(mfd->lock);

    if (rc < 0) {
        enif_release_binary(&buf);
        return errno_error(env, errno);
    }

    if (rc == 0) {
        enif_release_binary(&buf);
        return atom_eof;
    }

    if (!enif_realloc_binary(&buf, rc)) {
        enif_release_binary(&buf);
        return errno_error(env, ENOMEM);
    }

    ERL_NIF_TERM result = enif_make_binary(env, &buf);
    return enif_make_tuple2(env, atom_ok, result);
}


static ERL_NIF_TERM
pwrite_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    void *ptr;
    if (!enif_get_resource(env, argv[0], memfd_type, &ptr))
        return enif_make_badarg(env);

    unsigned int tmp;
    if (!enif_get_uint(env, argv[1], &tmp)) return enif_make_badarg(env);
    off_t offset = tmp;

    ErlNifBinary bin;
    if (!enif_inspect_binary(env, argv[2], &bin)) {
        if (!enif_inspect_iolist_as_binary(env, argv[1], &bin)) {
            return enif_make_badarg(env);
        }
    }

    memfd *mfd = (memfd *) ptr;

    enif_rwlock_rwlock(mfd->lock);

    if (mfd->closed) {
        enif_rwlock_rwunlock(mfd->lock);
        return errno_error(env, EBADF);
    }

    ssize_t rc = pwrite(mfd->fd, bin.data, bin.size, offset);

    enif_rwlock_rwunlock(mfd->lock);

    if (rc < 0) return errno_error(env, errno);

    if (rc != bin.size) return errno_error(env, ENOMEM);

    return atom_ok;
}



static ERL_NIF_TERM
from_fd_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifBinary bin;
    if (!enif_inspect_binary(env, argv[0], &bin) || bin.size != sizeof(int)) {
        return enif_make_badarg(env);
    }

    int fd = dup(*(int* )bin.data);

    if (fd == -1) return enif_make_badarg(env);

    int seals = fcntl(fd, F_GET_SEALS);
    if (seals == -1) {
        int error = errno;
        close(fd);
        return raise_errno_exception(env, error);
    }

    return allocate_memfd(env, fd);
}


static ERL_NIF_TERM
to_fd_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    void *ptr;
    if (!enif_get_resource(env, argv[0], memfd_type, &ptr))
        return enif_make_badarg(env);

    memfd *mfd = (memfd *) ptr;

    enif_rwlock_rlock(mfd->lock);

    if (mfd->closed) {
        enif_rwlock_runlock(mfd->lock);
        return raise_errno_exception(env, EBADF);
    }

    if (!is_owned(env, mfd)) {
        enif_rwlock_rwunlock(mfd->lock);
        return errno_error(env, EPERM);
    }

    ERL_NIF_TERM result = enif_make_resource_binary(env, ptr,
                                                    &mfd->fd, sizeof(int));

    enif_rwlock_runlock(mfd->lock);

    return result;
}


static void memfd_down(ErlNifEnv* env, void* obj, ErlNifPid* pid, ErlNifMonitor* mon)
{
    memfd* mfd = (memfd *) obj;

    enif_rwlock_rwlock(mfd->lock);

    if (mfd->closed) {
        enif_rwlock_rwunlock(mfd->lock);
        return;
    }

    close(mfd->fd);
    mfd->closed = true;
    enif_rwlock_rwunlock(mfd->lock);
}


static void memfd_dtor(ErlNifEnv *env, void *obj)
{
    memfd* mfd = (memfd *) obj;

    if (!mfd->closed) close(mfd->fd);

    enif_rwlock_destroy(mfd->lock);
}


static ErlNifFunc nifs[] =
{
    {"create_nif",     1, create_nif,   ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"close_nif",      1, close_nif,    ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"advise_nif",     4, advise_nif,   ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"allocate_nif",   3, allocate_nif, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"truncate_nif",   1, truncate_nif, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"truncate_nif",   2, truncate_nif, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"read_nif",       2, read_nif,     ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"write_nif",      2, write_nif,    ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"position_nif",   2, position_nif, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"pread_nif",      3, pread_nif,    ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"pwrite_nif",     3, pwrite_nif,   ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"from_fd_nif",    1, from_fd_nif,  ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"to_fd_nif",      1, to_fd_nif,    ERL_NIF_DIRTY_JOB_IO_BOUND},
};


static void init_atoms(ErlNifEnv* env)
{
    atom_ok = enif_make_atom(env, "ok");
    atom_error = enif_make_atom(env, "error");
    atom_true = enif_make_atom(env, "true");
    atom_false = enif_make_atom(env, "false");
    atom_bof = enif_make_atom(env, "bof");
    atom_cur = enif_make_atom(env, "cur");
    atom_eof = enif_make_atom(env, "eof");
    atom_normal = enif_make_atom(env, "normal");
    atom_sequential = enif_make_atom(env, "sequential");
    atom_random = enif_make_atom(env, "random");
    atom_no_reuse = enif_make_atom(env, "no_reuse");
    atom_will_need = enif_make_atom(env, "will_need");
    atom_dont_need = enif_make_atom(env, "dont_need");
}


static ErlNifResourceType* open_memfd_resource_type(ErlNifEnv *env,
                                                    ErlNifResourceFlags flags)
{
    ErlNifResourceTypeInit init;
    init.dtor = memfd_dtor;
    init.down = memfd_down;
    init.stop = NULL;

    return enif_open_resource_type_x(env, "memfd", &init, flags, NULL);
}


static int onload(ErlNifEnv *env, void **priv_data, ERL_NIF_TERM load_info)
{
    memfd_type = open_memfd_resource_type(env, ERL_NIF_RT_CREATE);
    if (memfd_type == NULL) return -1;

    init_atoms(env);

    return 0;
}


static int upgrade(ErlNifEnv* env, void** priv_data, void** old_priv_data,
                   ERL_NIF_TERM load_info)
{
    memfd_type = open_memfd_resource_type(env, ERL_NIF_RT_TAKEOVER);
    if (memfd_type == NULL) return -1;

    init_atoms(env);

    return 0;
}


ERL_NIF_INIT(memfd,nifs,onload,NULL,upgrade,NULL)


ERL_NIF_TERM allocate_memfd(ErlNifEnv* env, int fd)
{
    memfd *mfd = (memfd *) enif_alloc_resource(memfd_type, sizeof(memfd));

    enif_self(env, &mfd->owner);

    if (enif_monitor_process(env, mfd, &mfd->owner, &mfd->monitor) != 0) {
        close(fd);
        enif_release_resource(mfd);
        return raise_errno_exception(env, EOWNERDEAD);
    }

    mfd->fd = fd;
    mfd->lock = enif_rwlock_create((char*)"memfd");
    mfd->closed = false;

    ERL_NIF_TERM result = enif_make_resource(env, mfd);
    enif_release_resource(mfd);

    return result;
}


bool is_owned(ErlNifEnv* env, memfd* mfd)
{
    ErlNifPid self;
    enif_self(env, &self);

    return enif_is_identical(enif_make_pid(env, &mfd->owner), enif_make_pid(env, &self));
}


ERL_NIF_TERM errno_error(ErlNifEnv* env, int error)
{
    ERL_NIF_TERM reason = enif_make_atom(env, erl_errno_id(error));

    return enif_make_tuple2(env, atom_error, reason);
}


ERL_NIF_TERM raise_errno_exception(ErlNifEnv* env, int error)
{
    ERL_NIF_TERM reason = enif_make_atom(env, erl_errno_id(error));
    return enif_raise_exception(env, reason);
}
