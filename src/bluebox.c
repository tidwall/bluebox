#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <ctype.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/resource.h>
#include "hashmap.h"
#include "neco.h"

// #define USEBULKPOOL
// #define USEARGSPOOL

void *xmalloc(size_t nbytes) {
    void *ptr = malloc(nbytes);
    if (!ptr) {
        perror("malloc");
        abort();
    }
    return ptr;
}

void *xrealloc(void *ptr, size_t nbytes) {
    ptr = realloc(ptr, nbytes);
    if (!ptr) {
        perror("realloc");
        abort();
    }
    return ptr;
}

void xfree(void *ptr) {
    if (ptr) {
        free(ptr);
    }
}

struct bulkalloc {
    int rc;
    int len;
    char data[];
};

typedef char bulk_t;

#ifdef USEBULKPOOL
#define BULKPOOLMINLEN 32 // min len of bulk_t
#define BULKPOOLCAP 32     // max number of bulk_t in pool
static __thread int bulkpoollen = 0;
static __thread struct bulkalloc *bulkpool[BULKPOOLCAP];
#endif

bulk_t *bulk_alloc(size_t len) {
    size_t msize;
    struct bulkalloc *balloc = 0;
#ifdef USEBULKPOOL
    if (len <= BULKPOOLMINLEN) {
        if (bulkpoollen > 0) {
            balloc =  bulkpool[--bulkpoollen];
        } else {
            msize = sizeof(struct bulkalloc) + BULKPOOLMINLEN + 1;
            balloc = xmalloc(msize);
        }
    } else
#endif
    {
        msize = sizeof(struct bulkalloc) + len + 1;
    }
    if (!balloc) {
        balloc = xmalloc(msize);
    }
    balloc->len = len;
    balloc->rc = 0;
    balloc->data[len] = '\0';
    return balloc->data;
}

void bulk_retain(bulk_t *bulk) {
    if (!bulk) {
        return;
    }
    struct bulkalloc *balloc = (void*)(bulk-offsetof(struct bulkalloc, data));
    balloc->rc++;
}

void bulk_release(bulk_t *bulk) {
    if (!bulk) {
        return;
    }
    struct bulkalloc *balloc = (void*)(bulk-offsetof(struct bulkalloc, data));
    balloc->rc--;
    if (balloc->rc == -1) {
#ifdef USEBULKPOOL
        if (balloc->len <= BULKPOOLMINLEN && bulkpoollen < BULKPOOLCAP) {
            bulkpool[bulkpoollen++] = balloc;
        } else
#endif
        {
            xfree(balloc);
        }
    }
}

const char *bulk_data(const bulk_t *bulk) {
    return bulk;
}

int bulk_len(const bulk_t *bulk) {
    if (!bulk) {
        return 0;
    }
    struct bulkalloc *balloc = (void*)(bulk-offsetof(struct bulkalloc, data));
    return balloc->len;
}

int bulk_compare(const bulk_t *a, const bulk_t *b) {
    size_t alen = bulk_len(a);
    size_t blen = bulk_len(b);
    size_t nbytes = alen < blen ? alen : blen;
    int cmp = memcmp(bulk_data(a), bulk_data(b), nbytes);
    return cmp != 0 ? cmp : alen < blen ? -1 : alen > blen;
}


struct server {
    uint64_t sync_counter;
    struct hashmap *cmds;
    struct hashmap *keys;
};

struct client {
    neco_stream *stream;
};

struct key {
    bulk_t *key;
    bulk_t *value;
};

static uint64_t keyhash(const void *v, uint64_t seed0, uint64_t seed1) {
    const struct key *k = v;
    return hashmap_xxhash3(bulk_data(k->key), bulk_len(k->key), seed0, seed1);
}

static int keycompare(const void *va, const void *vb, void *udata) {
    const struct key *a = va;
    const struct key *b = vb;
    return bulk_compare(a->key, b->key);
}

struct cmd {
    char *name;
    bool (*func)(struct client *client, struct server *server, bulk_t **args, 
        int nargs);
};

static uint64_t cmdhash(const void *v, uint64_t seed0, uint64_t seed1) {
    const struct cmd *cmd = v;
    size_t len = strlen(cmd->name);
    return hashmap_xxhash3(cmd->name, len, seed0, seed1);
}

static int cmdcompare(const void *va, const void *vb, void *udata) {
    const struct cmd *a = va;
    const struct cmd *b = vb;
    return strcasecmp(a->name, b->name);
}

static bool read_telnet_args(struct client *client, bulk_t ***args_out,
    int *nargs_out)
{
    int argslen = 0;
    int argscap = 16;
    bulk_t **args = xmalloc(argscap*sizeof(bulk_t*));
    int linelen = 0;
    int linecap = 16;
    char *line = xmalloc(linecap);
    bool ok = false;
    while (1) {
        int c = neco_stream_read_byte(client->stream);
        if (c < 0 || linelen == 1048576) {
            break;
        }
        if (linelen == linecap) {
            line = xrealloc(line, linecap *= 2);
        }
        if (c == '\n') {
            if (linelen > 0 && line[linelen-1] == '\r') {
                linelen--;
                line[linelen] = '\0';
            } else {
                line[linelen] = '\0';
                linelen++;
            }
            ok = true;
            break;
        }
        line[linelen++] = c;
    }
    if (ok) {
        char *ptr = line;
        while (*ptr) {
            char *s, *e;
            if (*ptr == '\t' || *ptr == ' ') {
                ptr++;
                continue;
            }
            if (*ptr == '"' || *ptr == '\'') {
                char quote = *ptr;
                ptr++;
                s = ptr;
                e = 0;
                while (*ptr) {
                    if (*ptr == quote) {
                        e = ptr;
                        break;
                    }
                    ptr++;
                }
                if (!e) {
                    ok = false;
                    break;
                }
                ptr++;
            } else {
                s = ptr;
                while (*ptr) {
                    if (*ptr == '\t' || *ptr == ' ') {
                        break;
                    }
                    ptr++;
                }
                e = ptr;
            }
            bulk_t *arg = bulk_alloc(e-s);
            if (argslen == 0) {
                int i = 0;
                while (s < e) {
                    arg[i++] = tolower(*s);
                    s++;
                }
            } else {
                memcpy(arg, s, e-s);
            }
            if (argslen == argscap) {
                args = xrealloc(args, (argscap *= 2)*sizeof(bulk_t*));
            }
            args[argslen++] = arg;
        }
    }
    xfree(line);
    if (!ok) {
        for (int i = 0; i < argslen; i++) {
            bulk_release(args[i]);
        }
        xfree(args);
    } else {
        *args_out = args;
        *nargs_out = argslen;
    }
    return ok;
}

static bool read_integer0(struct client *client, long long *value) {
    char buf[32];
    int nbuf = 0;
    while (1) {
        int c = neco_stream_read_byte(client->stream);
        if (c < 0) {
            return false;
        }
        if (c == '\n') {
            break;
        }
        if (nbuf == sizeof(buf)-1) {
            // Way too much data for an integer
            return false;
        }
        buf[nbuf++] = c;
    }
    if (buf[nbuf-1] == '\r') {
        nbuf--;
    }
    buf[nbuf] = '\0';
    char *end;
    *value = strtoll(buf, &end, 10);
    if ((*value == 0 && errno != 0) || end < buf+nbuf) {
        // Bad integer
        return false;
    }
    return true;
}

static bool client_read_bulk(struct client *client, bool allow_null, 
    bulk_t **bulk)
{
    *bulk = NULL;
    int c = neco_stream_read_byte(client->stream);
    if (c < 0) {
        return false;
    }
    if (c != '$') {
        goto err_invalid_prefix;
    }
    long long nbytes;
    if (!read_integer0(client, &nbytes)) {
        goto err_invalid_bulk_length;
    }
    if (nbytes < -1) {
        // Invalid bulk size
        goto err_invalid_bulk_length;
    }
    if (nbytes == -1) {
        if (!allow_null) {
            goto err_invalid_bulk_length;
        }
        // Null type
        return true;
    }
    if (nbytes > 500*1024*1024) {
        goto err_invalid_bulk_length;
    }
    *bulk = bulk_alloc(nbytes);
    ssize_t n = neco_stream_readfull(client->stream, *bulk, nbytes);
    if (n != nbytes) {
        bulk_release(*bulk);
        return false;
    }
    if (neco_stream_read_byte(client->stream) < 0 || // \r
        neco_stream_read_byte(client->stream) < 0)   // \n
    {
        bulk_release(*bulk);
        return false;
    }
    return true;
err_invalid_prefix:
    {
        char msg[] = "-ERR Protocol error: expected '$', got '?'\r\n";
        if (c >= ' ' && c <= '~') {
            msg[strlen(msg)-4] = c;
        }
        neco_stream_write(client->stream, msg, strlen(msg));
        neco_stream_flush(client->stream);
        return false;
    }
err_invalid_bulk_length:
    {
        char msg[] = "-ERR Protocol error: invalid bulk length\r\n";
        neco_stream_write(client->stream, msg, strlen(msg));
        neco_stream_flush(client->stream);
        return false;
    }
}

#ifdef USEARGSPOOL
#define ARGSPOOLMINLEN 32  // min nargs
#define ARGSPOOLCAP 32     // max in pool
static __thread int argspoollen = 0;
static __thread bulk_t **argspool[ARGSPOOLCAP];
#endif


static bool client_read_args(struct client *client, bulk_t ***args_out, 
    int *nargs_out)
{
    int c = neco_stream_read_byte(client->stream);
    if (c < 0) {
        return false;
    }
    if (c != '*') {
        neco_stream_unread_byte(client->stream);
        bool ok = read_telnet_args(client, args_out, nargs_out);
        if (!ok) {
            char msg[] = "-ERR Protocol error: unbalanced "
                "quotes in request\r\n";
            neco_stream_write(client->stream, msg, strlen(msg));
            neco_stream_flush(client->stream);
        }
        return ok;
    }
    long long nargs;
    if (!read_integer0(client, &nargs)) {
        goto err_invalid_multibulk_length;
    }
    if (nargs <= 0) {
        *args_out = NULL;
        *nargs_out = 0;
        return true;
    }
    bulk_t **args;
#ifdef USEARGSPOOL
    if (nargs <= ARGSPOOLMINLEN) {
        if (argspoollen > 0) {
            args = argspool[--argspoollen];
        } else {
            args = xmalloc(ARGSPOOLMINLEN * sizeof(bulk_t *));
        }
    } else
#endif
    {
        args = xmalloc(nargs * sizeof(bulk_t *));
    }
    for (int i = 0; i < nargs; i++){ 
        bool ok = client_read_bulk(client, false, &args[i]);
        if (!ok) {
            for (int j = 0; j < i; j++) {
                bulk_release(args[j]);
            }
            xfree(args);
            return false;
        }
    }
    size_t blen = bulk_len(args[0]);
    for (size_t i = 0; i < blen; i++) {
        args[0][i] = tolower(args[0][i]);
    }
    *args_out = args;
    *nargs_out = nargs;
    return true;
err_invalid_multibulk_length:
    {
        char msg[] = "-ERR Protocol error: invalid multibulk length\r\n";
        neco_stream_write(client->stream, msg, strlen(msg));
        neco_stream_flush(client->stream);
        return false;
    }
}

static bool client_write_raw(struct client *client, const void *data, 
    size_t nbytes)
{
    ssize_t ret = neco_stream_write(client->stream, (void*)data, nbytes);
    return ret == (ssize_t)nbytes;
}

static bool client_write_cstr(struct client *client, const char *cstr) {
    return client_write_raw(client, cstr, strlen(cstr));
}

static void coclient(int argc, void *argv[]) {
    struct server *server = argv[0];
    int fd = *(int*)argv[1];
    struct client client = { 0 };
    neco_stream_make_buffered(&client.stream, fd);
    int plcmds = 0;
    while (1) {
        bulk_t **args;
        int nargs;
        if (!client_read_args(&client, &args, &nargs)) {
            break;
        }
        if (nargs <= 0) {
            continue;
        }
        struct cmd cmdkey = { .name = args[0] };
        const struct cmd *cmd = hashmap_get(server->cmds, &cmdkey);
        if (!cmd) {
            if (!client_write_cstr(&client, "-ERR unknown command\r\n")) {
                break;
            }
        } else if (!cmd->func(&client, server, args, nargs)) {
            break;
        }
        plcmds++;
        if (plcmds == 1000 || neco_stream_buffered_read_size(client.stream) == 0) {
            if (neco_stream_flush(client.stream) != NECO_OK) {
                break;
            }
            plcmds = 0;
        }
        // free the args
        for (int i = 0; i < nargs; i++) {
            bulk_release(args[i]);
        }
        bool must_free = true;
#ifdef USEARGSPOOL
        if (nargs <= ARGSPOOLMINLEN) {
            if (argspoollen < ARGSPOOLCAP) {
                argspool[argspoollen++] = args;
                must_free = false;
            }
        }
#endif
        if (must_free) {
            xfree(args);
        }
    }
    neco_stream_close(client.stream);
    // printf("client closed %d\n", nid);
}


static bool client_write_err_wrong_num_args(struct client *client, 
    const bulk_t *arg)
{
    return client_write_cstr(client, "-ERR wrong number of arguments\r\n");
}

static bool client_write_nil(struct client *client) {
    return client_write_cstr(client, "$-1\r\n");
}

static bool client_write_bulk(struct client *client, const bulk_t *bulk) {
    char prefix[32];
    snprintf(prefix, sizeof(prefix)-1, "$%d\r\n", bulk_len(bulk));
    if (!client_write_cstr(client, prefix)) {
        return false;
    }
    if (!client_write_raw(client, bulk_data(bulk), bulk_len(bulk))) {
        return false;
    }
    if (!client_write_cstr(client, "\r\n")) {
        return false;
    }
    return true;
}

static bool client_write_bulk_cstr(struct client *client, const char *cstr) {
    size_t nbytes = strlen(cstr);
    char prefix[32];
    snprintf(prefix, sizeof(prefix)-1, "$%zu\r\n", nbytes);
    if (!client_write_cstr(client, prefix)) {
        return false;
    }
    if (!client_write_raw(client, cstr, nbytes)) {
        return false;
    }
    if (!client_write_cstr(client, "\r\n")) {
        return false;
    }
    return true;
}

static bool client_write_ok(struct client *client) {
    return client_write_cstr(client, "+OK\r\n");
}

static bool client_write_int(struct client *client, int value) {
    char str[20];
    snprintf(str, sizeof(str), ":%d\r\n", value);
    return client_write_cstr(client, str);
}

static bool client_write_array(struct client *client, size_t nitems) {
    char str[20];
    snprintf(str, sizeof(str), "*%zu\r\n", nitems);
    return client_write_cstr(client, str);
}

static const void *key_set(struct server *server, const void *item) {
    return (void*)hashmap_set(server->keys, item);
}

static const void *key_get(struct server *server, const void *item) {
    return (void*)hashmap_get(server->keys, item);
}

static const void *key_del(struct server *server, const void *item) {
    return (void*)hashmap_delete(server->keys, item);
}

static bool cmdSET(struct client *client, struct server *server, 
    bulk_t **args, int nargs)
{
    if (nargs != 3) {
        return client_write_err_wrong_num_args(client, args[0]);
    }
    struct key key = { 
        .key = args[1],
        .value = args[2],
    };
    bulk_retain(key.key);
    bulk_retain(key.value);
    struct key *prev = (void*)key_set(server, &key);
    if (prev) {
        bulk_release(prev->key);
        bulk_release(prev->value);
    }
    return client_write_ok(client);
}

static bool cmdDEL(struct client *client, struct server *server, 
    bulk_t **args, int nargs)
{
    if (nargs < 2) {
        return client_write_err_wrong_num_args(client, args[0]);
    }
    int count = 0;
    for (int i = 1 ; i < nargs; i++) {
        if (key_del(server, &(struct key) { .key = args[i] })) {
            count++;
        }
    }
    return client_write_int(client, count);
}

static bool cmdGET(struct client *client, struct server *server, 
    bulk_t **args, int nargs)
{
    if (nargs != 2) {
        return client_write_err_wrong_num_args(client, args[0]);
    }
    // if (1) {
    //     return client_write_nil(client);
    // }
    const struct key keykey = { .key = args[1] };
    const struct key *key = key_get(server, &keykey);
    if (!key) {
        return client_write_nil(client);
    } else {
        return client_write_bulk(client, key->value);
    }
}

static bool cmdPING(struct client *client, struct server *server, 
    bulk_t **args, int nargs)
{
    switch (nargs) {
    case 1:
        return client_write_cstr(client, "+PONG\r\n");
    case 2:
        return client_write_bulk(client, args[1]);
    default:
        return client_write_err_wrong_num_args(client, args[0]);
    }
}

static bool cmdQUIT(struct client *client, struct server *server, 
    bulk_t **args, int nargs)
{
    client_write_cstr(client, "+OK\r\n");
    return false;
}

static bool cmdDBSIZE(struct client *client, struct server *server, 
    bulk_t **args, int nargs)
{
    return client_write_int(client, (int)hashmap_count(server->keys));
}

static bool cmdKEYS(struct client *client, struct server *server, 
    bulk_t **args, int nargs)
{
    size_t nkeys = hashmap_count(server->keys);
    if (!client_write_array(client, nkeys)) {
        return false;
    }
    void *item;
    size_t i = 0;
    while (hashmap_iter(server->keys, &i, &item)) {
        struct key *key = item;
        if (!client_write_bulk(client, key->key)) {
            return false;
        }
    }
    return true;
}

static void costats(int argc, void *argv[]) {
    neco_stats stats;
    char buf[100] = { 0 };
    char msg[100] = { 0 };
    while (1) {
        neco_sleep(NECO_SECOND/5);
        assert(neco_getstats(&stats) == NECO_OK);
        snprintf(buf, sizeof(buf), 
            "pid=%d coroutines=%zu sleepers=%zu "
            "waiters=%zu ", getpid(), stats.coroutines,
            stats.sleepers, stats.evwaiters);
        if (strcmp(buf, msg) != 0) {
            strcpy(msg, buf);
            printf("%s\n", msg);
        }
    }
}

void setmaxulimit(void) {
    struct rlimit limit;
    if (getrlimit(RLIMIT_NOFILE,&limit) == -1) {
        perror("getrlimit");
    } else {
        limit.rlim_cur = limit.rlim_max;
        if (setrlimit(RLIMIT_NOFILE, &limit) == -1) {
            perror("setrlimit");
        } else if (getrlimit(RLIMIT_NOFILE,&limit) == -1) {
            perror("getrlimit");
        } else {
            // printf("ulimit %llu\n", (unsigned long long)limit.rlim_cur);
        }
    }
}

int neco_main(int argc, char *argv[]) {
    int port = 9999;
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--port") == 0) {
            i++;
            if (i == argc) {
                fprintf(stderr, "missing port\n");
                exit(1);
            }
            port = atoi(argv[i]);
        }
    }

    setmaxulimit();

    printf("Using switch method: %s\n", neco_switch_method());

    struct server server = { 0 };
    server.cmds = hashmap_new_with_allocator(xmalloc, xrealloc, xfree,
        sizeof(struct cmd), 0, 0, 0, cmdhash, cmdcompare, 0, 0);
    server.keys = hashmap_new_with_allocator(xmalloc, xrealloc, xfree,
        sizeof(struct key), 0, 0, 0, keyhash, keycompare, 0, 0);

    hashmap_set(server.cmds, &(struct cmd){ .name = "del", .func = cmdDEL });
    hashmap_set(server.cmds, &(struct cmd){ .name = "set", .func = cmdSET });
    hashmap_set(server.cmds, &(struct cmd){ .name = "get", .func = cmdGET });
    hashmap_set(server.cmds, &(struct cmd){ .name = "ping", .func = cmdPING });
    hashmap_set(server.cmds, &(struct cmd){ .name = "quit", .func = cmdQUIT });
    hashmap_set(server.cmds, &(struct cmd){ .name = "dbsize", .func = cmdDBSIZE });
    hashmap_set(server.cmds, &(struct cmd){ .name = "keys", .func = cmdKEYS });
    
    char addr[64];
    snprintf(addr, sizeof(addr), "0.0.0.0:%d", port);
    int sockfd = neco_serve("tcp", addr);
    if (sockfd == -1) {
        perror("neco_serve");
        exit(1);
    }
    neco_start(costats, 0);

    printf("Started BlueBox on port %d\n", port);
    while (1) {
        int fd = neco_accept(sockfd, 0, 0);
        if (fd == -1) {
            perror("accept");
        } else {
            int err = neco_start(coclient, 2, &server, &fd);
            if (err != NECO_OK) {
                printf("start: %s\n", neco_strerror(err));
            }
        }
    }
    return 0;
}
