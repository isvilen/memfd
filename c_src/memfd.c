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


typedef struct {
    ErlNifResourceType* memfd_type;
    ErlNifResourceType* memap_type;
    ERL_NIF_TERM atom_ok;
    ERL_NIF_TERM atom_error;
    ERL_NIF_TERM atom_true;
    ERL_NIF_TERM atom_false;
    ERL_NIF_TERM atom_bof;
    ERL_NIF_TERM atom_cur;
    ERL_NIF_TERM atom_eof;
    ERL_NIF_TERM atom_normal;
    ERL_NIF_TERM atom_sequential;
    ERL_NIF_TERM atom_random;
    ERL_NIF_TERM atom_no_reuse;
    ERL_NIF_TERM atom_will_need;
    ERL_NIF_TERM atom_dont_need;
} memfd_data;


typedef struct {
    ErlNifRWLock *lock;
    int fd;
    bool sealed;
} memfd;


typedef struct {
    void *buf;
    size_t size;
} memap;


static memfd* allocate_memfd(memfd_data *data, int fd);
static memap* allocate_memap(memfd_data *data, void *buf, size_t size);
static ERL_NIF_TERM errno_error(ErlNifEnv* env, int error);
static bool get_location(ErlNifEnv* env, ERL_NIF_TERM arg,
                         off_t *offset,  int *whence);


static ERL_NIF_TERM
create_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    unsigned int size;

    if (!enif_get_list_length(env, argv[0], &size)) {
        return enif_make_badarg(env);
    }

    char *name = alloca(size+1);

    if (enif_get_string(env, argv[0], name, size+1, ERL_NIF_LATIN1) < 1) {
        return enif_make_badarg(env);
    }

    int fd = syscall(__NR_memfd_create, name, MFD_CLOEXEC | MFD_ALLOW_SEALING);
    if (fd < 0) {
        ERL_NIF_TERM reason = enif_make_atom(env, erl_errno_id(errno));
        return enif_raise_exception(env, reason);
    }

    memfd_data *data = (memfd_data *) enif_priv_data(env);

    memfd *mfd = allocate_memfd(data, fd);

    ERL_NIF_TERM result = enif_make_resource(env, mfd);
    enif_release_resource(mfd);

    return result;
}


static ERL_NIF_TERM
close_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    memfd_data *data = (memfd_data *) enif_priv_data(env);

    void *ptr;
    if (!enif_get_resource(env, argv[0], data->memfd_type, &ptr))
        return enif_make_badarg(env);

    memfd *mfd = (memfd *) ptr;

    enif_rwlock_rwlock(mfd->lock);

    if (mfd->fd == -1) {
        enif_rwlock_rwunlock(mfd->lock);
        return errno_error(env, EBADF);
    }

    if (close(mfd->fd) < 0) {
        mfd->fd = -1;
        enif_rwlock_rwunlock(mfd->lock);
        return errno_error(env, errno);
    }

    mfd->fd = -1;
    enif_rwlock_rwunlock(mfd->lock);

    return data->atom_ok;
}


static ERL_NIF_TERM
advise_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    memfd_data *data = (memfd_data *) enif_priv_data(env);

    void *ptr;
    if (!enif_get_resource(env, argv[0], data->memfd_type, &ptr))
        return enif_make_badarg(env);

    unsigned int tmp;
    if (!enif_get_uint(env, argv[1], &tmp)) return enif_make_badarg(env);
    off_t offset = tmp;

    if (!enif_get_uint(env, argv[2], &tmp)) return enif_make_badarg(env);
    off_t len = tmp;

    int advice = 0;
    if (enif_compare(argv[3], data->atom_normal) == 0)
        advice = POSIX_FADV_NORMAL;
    else if (enif_compare(argv[3], data->atom_sequential) == 0)
        advice = POSIX_FADV_SEQUENTIAL;
    else if (enif_compare(argv[3], data->atom_random) == 0)
        advice = POSIX_FADV_RANDOM;
    else if (enif_compare(argv[3], data->atom_no_reuse) == 0)
        advice = POSIX_FADV_NOREUSE;
    else if (enif_compare(argv[3], data->atom_will_need) == 0)
        advice = POSIX_FADV_WILLNEED;
    else if (enif_compare(argv[3], data->atom_dont_need) == 0)
        advice = POSIX_FADV_DONTNEED;
    else return enif_make_badarg(env);

    memfd *mfd = (memfd *) ptr;

    enif_rwlock_rlock(mfd->lock);

    if (mfd->fd == -1) {
        enif_rwlock_runlock(mfd->lock);
        return errno_error(env, EBADF);
    }

    int rc = posix_fadvise(mfd->fd, offset, len, advice);

    enif_rwlock_runlock(mfd->lock);

    if (rc != 0) return errno_error(env, rc);

    return data->atom_ok;
}


static ERL_NIF_TERM
allocate_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    memfd_data *data = (memfd_data *) enif_priv_data(env);

    void *ptr;
    if (!enif_get_resource(env, argv[0], data->memfd_type, &ptr))
        return enif_make_badarg(env);

    unsigned int tmp;
    if (!enif_get_uint(env, argv[1], &tmp)) return enif_make_badarg(env);
    off_t offset = tmp;

    if (!enif_get_uint(env, argv[2], &tmp)) return enif_make_badarg(env);
    off_t len = tmp;

    memfd *mfd = (memfd *) ptr;

    enif_rwlock_rwlock(mfd->lock);

    if (mfd->fd == -1) {
        enif_rwlock_rwunlock(mfd->lock);
        return errno_error(env, EBADF);
    }

    int rc = posix_fallocate(mfd->fd, offset, len);

    enif_rwlock_rwunlock(mfd->lock);

    if (rc != 0) return errno_error(env, rc);

    return data->atom_ok;
}


static ERL_NIF_TERM
truncate_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    memfd_data *data = (memfd_data *) enif_priv_data(env);

    void *ptr;
    if (!enif_get_resource(env, argv[0], data->memfd_type, &ptr))
        return enif_make_badarg(env);

    off_t length = (off_t) -1;

    if (argc > 1) {
        unsigned int tmp;
        if (!enif_get_uint(env, argv[1], &tmp)) return enif_make_badarg(env);
        length = tmp;
    }

    memfd *mfd = (memfd *) ptr;

    enif_rwlock_rwlock(mfd->lock);

    if (mfd->fd == -1) {
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

    return data->atom_ok;
}


static ERL_NIF_TERM
read_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    memfd_data *data = (memfd_data *) enif_priv_data(env);

    void *ptr;
    if (!enif_get_resource(env, argv[0], data->memfd_type, &ptr))
        return enif_make_badarg(env);

    unsigned int size;
    if (!enif_get_uint(env, argv[1], &size)) return enif_make_badarg(env);

    ErlNifBinary buf;
    if (!enif_alloc_binary(size, &buf)) return errno_error(env, ENOMEM);

    memfd *mfd = (memfd *) ptr;

    enif_rwlock_rlock(mfd->lock);

    if (mfd->fd == -1) {
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
        return data->atom_eof;
    }

    if (!enif_realloc_binary(&buf, rc)) {
        enif_release_binary(&buf);
        return errno_error(env, ENOMEM);
    }

    ERL_NIF_TERM result = enif_make_binary(env, &buf);
    return enif_make_tuple2(env, data->atom_ok, result);
}


static ERL_NIF_TERM
write_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    memfd_data *data = (memfd_data *) enif_priv_data(env);

    void *ptr;
    if (!enif_get_resource(env, argv[0], data->memfd_type, &ptr))
        return enif_make_badarg(env);

    ErlNifBinary bin;
    if (!enif_inspect_binary(env, argv[1], &bin)) {
        if (!enif_inspect_iolist_as_binary(env, argv[1], &bin)) {
            return enif_make_badarg(env);
        }
    }

    memfd *mfd = (memfd *) ptr;

    enif_rwlock_rwlock(mfd->lock);

    if (mfd->fd == -1) {
        enif_rwlock_rwunlock(mfd->lock);
        return errno_error(env, EBADF);
    }

    ssize_t rc = write(mfd->fd, bin.data, bin.size);

    enif_rwlock_rwunlock(mfd->lock);

    if (rc < 0) return errno_error(env, errno);

    if (rc != bin.size) return errno_error(env, ENOMEM);

    return data->atom_ok;
}


static ERL_NIF_TERM
position_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    memfd_data *data = (memfd_data *) enif_priv_data(env);

    void *ptr;
    if (!enif_get_resource(env, argv[0], data->memfd_type, &ptr))
        return enif_make_badarg(env);

    off_t offset;
    int whence;
    if (!get_location(env, argv[1], &offset, &whence))
        return enif_make_badarg(env);

    memfd *mfd = (memfd *) ptr;

    enif_rwlock_rlock(mfd->lock);

    if (mfd->fd == -1) {
        enif_rwlock_runlock(mfd->lock);
        return errno_error(env, EBADF);
    }

    off_t np = lseek(mfd->fd, offset, whence);

    enif_rwlock_runlock(mfd->lock);

    if (np == (off_t) -1) return errno_error(env, errno);

    ERL_NIF_TERM result = enif_make_uint(env, np);
    return enif_make_tuple2(env, data->atom_ok, result);
}


static ERL_NIF_TERM
pread_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    memfd_data *data = (memfd_data *) enif_priv_data(env);

    void *ptr;
    if (!enif_get_resource(env, argv[0], data->memfd_type, &ptr))
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

    if (mfd->fd == -1) {
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
        return data->atom_eof;
    }

    if (!enif_realloc_binary(&buf, rc)) {
        enif_release_binary(&buf);
        return errno_error(env, ENOMEM);
    }

    ERL_NIF_TERM result = enif_make_binary(env, &buf);
    return enif_make_tuple2(env, data->atom_ok, result);
}


static ERL_NIF_TERM
pwrite_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    memfd_data *data = (memfd_data *) enif_priv_data(env);

    void *ptr;
    if (!enif_get_resource(env, argv[0], data->memfd_type, &ptr))
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

    if (mfd->fd == -1) {
        enif_rwlock_rwunlock(mfd->lock);
        return errno_error(env, EBADF);
    }

    ssize_t rc = pwrite(mfd->fd, bin.data, bin.size, offset);

    enif_rwlock_rwunlock(mfd->lock);

    if (rc < 0) return errno_error(env, errno);

    if (rc != bin.size) return errno_error(env, ENOMEM);

    return data->atom_ok;
}


static ERL_NIF_TERM
mmap_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    memfd_data *data = (memfd_data *) enif_priv_data(env);

    void *ptr;
    if (!enif_get_resource(env, argv[0], data->memfd_type, &ptr))
        return enif_make_badarg(env);

    memfd *mfd = (memfd *) ptr;

    enif_rwlock_rwlock(mfd->lock);

    if (mfd->fd == -1) {
        enif_rwlock_rwunlock(mfd->lock);
        return errno_error(env, EBADF);
    }

    if (!mfd->sealed) {
        if (fcntl(mfd->fd, F_ADD_SEALS, F_SEAL_SHRINK) == -1) {
            enif_rwlock_rwunlock(mfd->lock);
            return errno_error(env, errno);
        }
        mfd->sealed = true;
    }

    struct stat st;
    if (fstat(mfd->fd, &st) < 0) {
        enif_rwlock_rwunlock(mfd->lock);
        return errno_error(env, errno);
    }

    void *buf = mmap(NULL, st.st_size, PROT_READ | PROT_WRITE, MAP_SHARED,
                     mfd->fd, 0);

    enif_rwlock_rwunlock(mfd->lock);

    if (buf == MAP_FAILED) return errno_error(env, errno);

    memap *map = allocate_memap(data, buf, st.st_size);

    ERL_NIF_TERM result = enif_make_resource(env, map);
    enif_release_resource(map);

    return enif_make_tuple2(env, data->atom_ok, result);
}


static ERL_NIF_TERM
mmap_buffer_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    memfd_data *data = (memfd_data *) enif_priv_data(env);

    void *ptr;
    if (!enif_get_resource(env, argv[0], data->memap_type, &ptr))
        return enif_make_badarg(env);

    memap *map = (memap *)ptr;

    return enif_make_resource_binary(env, ptr, map->buf, map->size);
}


static ERL_NIF_TERM
from_fd_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifBinary bin;
    if (!enif_inspect_binary(env, argv[0], &bin) || bin.size != sizeof(int)) {
        return enif_make_badarg(env);
    }

    int fd = dup(*(int* )bin.data);

    if (fd == -1) {
        return enif_make_badarg(env);
    }

    int seals = fcntl(fd, F_GET_SEALS);
    if (seals == -1) {
        int error = errno;
        close(fd);
        ERL_NIF_TERM reason = enif_make_atom(env, erl_errno_id(error));
        return enif_raise_exception(env, reason);
    }

    memfd_data *data = (memfd_data *) enif_priv_data(env);

    memfd *mfd = allocate_memfd(data, fd);
    mfd->sealed = (seals & (F_SEAL_SHRINK | F_SEAL_SEAL)) != 0;

    ERL_NIF_TERM result = enif_make_resource(env, mfd);
    enif_release_resource(mfd);

    return result;
}


static ERL_NIF_TERM
to_fd_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    memfd_data *data = (memfd_data *) enif_priv_data(env);

    void *ptr;
    if (!enif_get_resource(env, argv[0], data->memfd_type, &ptr))
        return enif_make_badarg(env);

    memfd *mfd = (memfd *) ptr;

    enif_rwlock_rlock(mfd->lock);

    if (mfd->fd == -1) {
        enif_rwlock_runlock(mfd->lock);
        ERL_NIF_TERM reason = enif_make_atom(env, erl_errno_id(EBADF));
        return enif_raise_exception(env, reason);
    }

    ERL_NIF_TERM result = enif_make_resource_binary(env, ptr,
                                                    &mfd->fd, sizeof(int));

    enif_rwlock_runlock(mfd->lock);

    return result;
}


static void memfd_dtor(ErlNifEnv *env, void *obj)
{
    memfd *mfd = (memfd *) obj;

    if (mfd->fd != -1) {
        close(mfd->fd);
        mfd->fd = -1;
    }

    if (mfd->lock != 0) {
        enif_rwlock_destroy(mfd->lock);
        mfd->lock = 0;
    }
}


static void memap_dtor(ErlNifEnv *env, void *obj)
{
    memap *map = (memap *) obj;

    if (map->buf != NULL) {
        munmap(map->buf, map->size);
        map->buf = NULL;
    }
}


static ErlNifFunc nifs[] =
{
    {"create_nif",     1, create_nif},
    {"close_nif",      1, close_nif},
    {"advise_nif",     4, advise_nif},
    {"allocate_nif",   3, allocate_nif},
    {"truncate_nif",   1, truncate_nif},
    {"truncate_nif",   2, truncate_nif},
    {"read_nif",       2, read_nif},
    {"write_nif",      2, write_nif},
    {"position_nif",   2, position_nif},
    {"pread_nif",      3, pread_nif},
    {"pwrite_nif",     3, pwrite_nif},
    {"mmap_nif",       1, mmap_nif},
    {"mmap_buffer_nif",1, mmap_buffer_nif},
    {"from_fd_nif",    1, from_fd_nif},
    {"to_fd_nif",      1, to_fd_nif},
};


static bool open_resource_types(ErlNifEnv *env, memfd_data *data)
{
    data->memfd_type = enif_open_resource_type(env, NULL, "memfd", memfd_dtor,
                                               ERL_NIF_RT_CREATE |
                                               ERL_NIF_RT_TAKEOVER, NULL);

    data->memap_type = enif_open_resource_type(env, NULL, "memap", memap_dtor,
                                               ERL_NIF_RT_CREATE |
                                               ERL_NIF_RT_TAKEOVER, NULL);

    if (!data->memfd_type || !data->memap_type) return false;

    return true;
}


static int onload(ErlNifEnv *env, void **priv_data, ERL_NIF_TERM load_info)
{
    memfd_data *data = (memfd_data *) enif_alloc(sizeof(memfd_data));

    if (!open_resource_types(env, data)) {
        enif_free(data);
        return -1;
    }

    data->atom_ok = enif_make_atom(env, "ok");
    data->atom_error = enif_make_atom(env, "error");
    data->atom_true = enif_make_atom(env, "true");
    data->atom_false = enif_make_atom(env, "false");
    data->atom_bof = enif_make_atom(env, "bof");
    data->atom_cur = enif_make_atom(env, "cur");
    data->atom_eof = enif_make_atom(env, "eof");
    data->atom_normal = enif_make_atom(env, "normal");
    data->atom_sequential = enif_make_atom(env, "sequential");
    data->atom_random = enif_make_atom(env, "random");
    data->atom_no_reuse = enif_make_atom(env, "no_reuse");
    data->atom_will_need = enif_make_atom(env, "will_need");
    data->atom_dont_need = enif_make_atom(env, "dont_need");

    *priv_data = data;

    return 0;
}


static int upgrade(ErlNifEnv* env, void** priv_data, void** old_priv_data,
                   ERL_NIF_TERM load_info)
{
    if (!open_resource_types(env, *old_priv_data))
        return -1;

    *priv_data = *old_priv_data;
    *old_priv_data = NULL;

    return 0;
}


static void unload(ErlNifEnv *env, void *priv_data)
{
    enif_free(priv_data);
}


ERL_NIF_INIT(memfd,nifs,onload,NULL,upgrade,unload)


memfd* allocate_memfd(memfd_data *data, int fd)
{
    memfd *mfd = (memfd *) enif_alloc_resource(data->memfd_type, sizeof(memfd));

    mfd->fd = fd;
    mfd->lock = enif_rwlock_create((char*)"memfd");
    mfd->sealed = false;

    return mfd;
}


memap* allocate_memap(memfd_data *data, void *buf, size_t size)
{
    memap *map = (memap *) enif_alloc_resource(data->memap_type, sizeof(memap));

    map->buf = buf;
    map->size = size;

    return map;
}


ERL_NIF_TERM errno_error(ErlNifEnv* env, int error)
{
    memfd_data *data = (memfd_data *) enif_priv_data(env);

    ERL_NIF_TERM reason = enif_make_atom(env, erl_errno_id(error));

    return enif_make_tuple2(env, data->atom_error, reason);
}


bool get_location(ErlNifEnv* env, ERL_NIF_TERM arg, off_t *offset,  int *whence)
{
    memfd_data *data = (memfd_data *) enif_priv_data(env);

    int arity;
    const ERL_NIF_TERM  *elems;

    if (!enif_get_tuple(env, arg, &arity, &elems)) return false;
    if (arity != 2) return false;

    if (enif_compare(elems[0], data->atom_bof) == 0)
        *whence = SEEK_SET;
    else if (enif_compare(elems[0], data->atom_cur) == 0)
        *whence = SEEK_CUR;
    else if (enif_compare(elems[0], data->atom_eof) == 0)
        *whence = SEEK_END;
    else return false;

    long tmp;
    if (!enif_get_long(env, elems[1], &tmp)) return false;
    *offset = tmp;

    return true;
}
