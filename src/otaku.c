#include <mpi.h>

#include <stdbool.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
    TAG_REQUEST = 1,
    TAG_ACK = 2,
    TAG_ENTER = 3,
    TAG_RELEASE = 4,
    TAG_FAINT = 5,
    TAG_BANNED = 6
};

typedef struct {
    int ts;
    int pid;
} RequestId;

typedef struct {
    int ts;
    int pid;
    int smell;
} Entry;

typedef struct {
    int ts;
    int smell;
    bool active;
    bool inside;
} ProcRequest;

int world_size = 0;
int world_rank = 0;
int lamport = 0;

int stations = 0;
int max_smell = 0;
int guard_limit = 0;

int smell = 1;

ProcRequest *requests = NULL;
Entry *history = NULL;
int history_count = 0;
int history_cap = 0;
long long history_sum = 0;

long long dose = 0;
RequestId cut_id = {-1, -1};
RequestId my_request_id = {-1, -1};
int last_faint_id = 0;
int next_faint_seq = 1;

bool want_enter = false;
bool inside = false;
bool faint_sent = false;
bool excluded = false;
bool slow = true;
int ack_count = 0;
int work_steps_left = 0;
int banned_count = 0;

void log_msg(const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    printf("[%d] [t%d] ", world_rank + 1, lamport);
    vprintf(fmt, ap);
    printf("\n");
    fflush(stdout);
    va_end(ap);
}

void sleep_if_slow(void) {
    if (!slow) {
        return;
    }
    double start = MPI_Wtime();
    while (MPI_Wtime() - start < 1.0) {
        /* busy wait for readability */
    }
}

int id_cmp(RequestId a, RequestId b) {
    if (a.ts != b.ts) {
        return (a.ts < b.ts) ? -1 : 1;
    }
    if (a.pid != b.pid) {
        return (a.pid < b.pid) ? -1 : 1;
    }
    return 0;
}

bool id_leq(RequestId a, RequestId b) {
    return id_cmp(a, b) <= 0;
}

void clock_send_tick(void) {
    lamport++;
}

void clock_recv_tick(int received_ts) {
    if (lamport < received_ts) {
        lamport = received_ts;
    }
    lamport++;
}

int rand_range(int min_value, int max_value) {
    return min_value + rand() % (max_value - min_value + 1);
}

void ensure_history_capacity(void) {
    if (history_count < history_cap) {
        return;
    }

    int new_cap = history_cap == 0 ? 16 : history_cap * 2;
    Entry *new_hist = (Entry *)realloc(history, (size_t)new_cap * sizeof(Entry));
    if (!new_hist) {
        fprintf(stderr, "Out of memory while growing enter history\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    history = new_hist;
    history_cap = new_cap;
}

void insert_history(Entry e) {
    ensure_history_capacity();

    int pos = history_count;
    while (pos > 0 && id_cmp((RequestId){history[pos - 1].ts, history[pos - 1].pid}, (RequestId){e.ts, e.pid}) > 0) {
        history[pos] = history[pos - 1];
        pos--;
    }
    history[pos] = e;
    history_count++;
    history_sum += e.smell;
}

void trim_history_prefix(RequestId border) {
    int keep_from = 0;
    while (keep_from < history_count) {
        RequestId current = (RequestId){history[keep_from].ts, history[keep_from].pid};
        if (id_cmp(current, border) > 0) {
            break;
        }
        history_sum -= history[keep_from].smell;
        keep_from++;
    }

    if (keep_from > 0) {
        memmove(history, history + keep_from, (size_t)(history_count - keep_from) * sizeof(Entry));
        history_count -= keep_from;
    }
}

void pack_send(int dest, int tag, int a, int b, int c) {
    int msg[4] = {a, b, c, 0};
    MPI_Send(msg, 4, MPI_INT, dest, tag, MPI_COMM_WORLD);
    sleep_if_slow();
}

void broadcast(int tag, int a, int b, int c) {
    for (int dest = 0; dest < world_size; ++dest) {
        if (dest == world_rank) {
            continue;
        }
        pack_send(dest, tag, a, b, c);
    }
}

void broadcast_banned(void) {
    for (int dest = 0; dest < world_size; ++dest) {
        if (dest == world_rank) {
            continue;
        }
        pack_send(dest, TAG_BANNED, world_rank, 0, 0);
    }
}

int active_request_compare(const void *lhs, const void *rhs) {
    int a = *(const int *)lhs;
    int b = *(const int *)rhs;
    RequestId ia = {(int)requests[a].ts, a};
    RequestId ib = {(int)requests[b].ts, b};
    return id_cmp(ia, ib);
}

int build_active_list(int *active_pids) {
    int count = 0;
    for (int pid = 0; pid < world_size; ++pid) {
        if (requests[pid].active) {
            active_pids[count++] = pid;
        }
    }
    qsort(active_pids, (size_t)count, sizeof(int), active_request_compare);
    return count;
}

bool self_is_allowed(void) {
    int *active_pids = (int *)malloc((size_t)world_size * sizeof(int));
    if (!active_pids) {
        fprintf(stderr, "Out of memory while building active request list\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    int active_count = build_active_list(active_pids);
    int admitted = 0;
    long long smell_sum = 0;
    bool allowed = false;

    for (int i = 0; i < active_count; ++i) {
        int pid = active_pids[i];
        if (admitted < stations && smell_sum + requests[pid].smell <= max_smell) {
            if (pid == world_rank) {
                allowed = true;
                break;
            }
            admitted++;
            smell_sum += requests[pid].smell;
        } else {
            break;
        }
    }

    free(active_pids);
    return allowed;
}

void apply_faint(RequestId trigger, int faint_id) {
    if (faint_id <= last_faint_id) {
        return;
    }

    last_faint_id = faint_id;
    dose -= guard_limit;
    if (dose < 0) {
        dose = 0;
    }
    trim_history_prefix(trigger);
    cut_id = trigger;
}

bool compute_trigger(RequestId *trigger_out, long long *accumulated_out) {
    long long base_dose = dose - history_sum;
    if (base_dose < 0) {
        base_dose = 0;
    }
    long long running = base_dose;

    for (int i = 0; i < history_count; ++i) {
        running += history[i].smell;
        if (running > guard_limit) {
            trigger_out->ts = history[i].ts;
            trigger_out->pid = history[i].pid;
            if (accumulated_out) {
                *accumulated_out = running;
            }
            return true;
        }
    }

    return false;
}

void maybe_send_faint(void) {
    RequestId trigger;
    long long accumulated = 0;
    if (!compute_trigger(&trigger, &accumulated)) {
        return;
    }

    if (id_cmp(trigger, cut_id) <= 0) {
        return;
    }

    if (trigger.ts != my_request_id.ts || trigger.pid != my_request_id.pid) {
        return;
    }

    if (faint_sent) {
        return;
    }

    int faint_id = (world_rank + 1) * 1000000 + next_faint_seq++;
    faint_sent = true;
    log_msg("Wysylam FAINT dla wejscia (%d,%d), zakumulowany smrod=%lld", trigger.ts, trigger.pid, accumulated);
    apply_faint(trigger, faint_id);
    broadcast(TAG_FAINT, trigger.ts, trigger.pid, faint_id);
}

void handle_request_message(int ts, int pid, int smell_value) {
    clock_recv_tick(ts);
    requests[pid].ts = ts;
    requests[pid].smell = smell_value;
    requests[pid].active = true;
    if (pid == world_rank) {
        return;
    }
    pack_send(pid, TAG_ACK, lamport, world_rank, 0);
}

void handle_enter_message(int ts, int pid, int smell_value) {
    clock_recv_tick(ts);
    RequestId entry_id = {ts, pid};
    if (id_leq(entry_id, cut_id)) {
        return;
    }
    if (!requests[pid].active || requests[pid].ts != ts) {
        return;
    }

    requests[pid].inside = true;
    insert_history((Entry){ts, pid, smell_value});
    dose += smell_value;
    maybe_send_faint();
}

void handle_release_message(int ts, int pid) {
    clock_recv_tick(ts);
    if (!requests[pid].active || requests[pid].ts != ts) {
        return;
    }
    requests[pid].inside = false;
    requests[pid].active = false;
}

void handle_faint_message(int trigger_ts, int trigger_pid, int faint_id) {
    clock_recv_tick(trigger_ts);
    if (faint_id <= last_faint_id) {
        return;
    }

    RequestId trigger = {trigger_ts, trigger_pid};
    apply_faint(trigger, faint_id);
}

void handle_banned_message(int pid) {
    banned_count++;
}

void pump_messages(void) {
    int flag = 0;
    MPI_Status status;

    for (;;) {
        MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &flag, &status);
        if (!flag) {
            break;
        }

        int msg[4] = {0, 0, 0, 0};
        MPI_Recv(msg, 4, MPI_INT, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD, &status);

        switch (status.MPI_TAG) {
            case TAG_REQUEST:
                handle_request_message(msg[0], msg[1], msg[2]);
                break;
            case TAG_ACK:
                if (want_enter && !inside) {
                    ack_count++;
                    if (ack_count == world_size - 1) {
                        log_msg("Odebralem wszystkie ACK (%d/%d)", ack_count, world_size - 1);
                    }
                }
                break;
            case TAG_ENTER:
                handle_enter_message(msg[0], msg[1], msg[2]);
                break;
            case TAG_RELEASE:
                handle_release_message(msg[0], msg[1]);
                break;
            case TAG_FAINT:
                handle_faint_message(msg[0], msg[1], msg[2]);
                break;
            case TAG_BANNED:
                handle_banned_message(msg[0]);
                break;
            default:
                break;
        }
    }
}

void start_request(void) {
    clock_send_tick();
    my_request_id.ts = lamport;
    my_request_id.pid = world_rank;
    want_enter = true;
    faint_sent = false;
    ack_count = 0;

    requests[world_rank].ts = my_request_id.ts;
    requests[world_rank].smell = smell;
    requests[world_rank].active = true;
    requests[world_rank].inside = false;

    log_msg("Rozpoczynam staranie o sekcje krytyczna (smrod=%d)", smell);
    broadcast(TAG_REQUEST, my_request_id.ts, world_rank, smell);
}

void enter_room(void) {
    clock_send_tick();
    inside = true;
    requests[world_rank].inside = true;

    log_msg("Wchodze do sekcji krytycznej");

    insert_history((Entry){my_request_id.ts, world_rank, smell});
    dose += smell;

    broadcast(TAG_ENTER, my_request_id.ts, world_rank, smell);
    maybe_send_faint();

    work_steps_left = 3 + (world_rank % 3);
}

void release_room(void) {
    clock_send_tick();
    log_msg("Wychodze z sekcji krytycznej");

    broadcast(TAG_RELEASE, my_request_id.ts, world_rank, 0);

    inside = false;
    want_enter = false;
    requests[world_rank].active = false;
    requests[world_rank].inside = false;
    work_steps_left = 0;

    smell += rand_range(1, 5);
    if (smell > max_smell) {
        excluded = true;
        log_msg("Otaku %d zostal zbanowany z Pyrconu, aktualny smrod=%d", world_rank + 1, smell);
        banned_count++;
        broadcast_banned();
    }
}

void try_progress(void) {
    if (excluded) {
        return;
    }

    if (!want_enter && !inside) {
        start_request();
    }

    if (want_enter && !inside && ack_count >= world_size - 1) {
        if (self_is_allowed()) {
            log_msg("Mam komplet ACK, wysylam ENTER");
            enter_room();
        }
    }

    if (inside) {
        if (work_steps_left > 0) {
            work_steps_left--;
        }
        if (work_steps_left == 0) {
            release_room();
        }
    }
}

int parse_int(const char *text, const char *name) {
    char *end = NULL;
    long value = strtol(text, &end, 10);
    if (!text[0] || (end && *end != '\0')) {
        fprintf(stderr, "Niepoprawna wartosc parametru %s: %s\n", name, text);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    if (value < 0 || value > 1000000000L) {
        fprintf(stderr, "Wartosc parametru %s poza zakresem: %s\n", name, text);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    return (int)value;
}

int main(int argc, char **argv) {
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    if (argc < 4) {
        if (world_rank == 0) {
            fprintf(stderr, "Uzycie: %s S M X [slow]\n", argv[0]);
        }
        MPI_Finalize();
        return 1;
    }

    stations = parse_int(argv[1], "S");
    max_smell = parse_int(argv[2], "M");
    guard_limit = parse_int(argv[3], "X");
    if (argc >= 5) {
        slow = parse_int(argv[4], "slow") != 0;
    }
    if (stations <= 0 || guard_limit <= 0) {
        if (world_rank == 0) {
            fprintf(stderr, "S i X musza byc dodatnie.\n");
        }
        MPI_Finalize();
        return 1;
    }

    if (world_rank == 0) {
        printf("Start: N=%d, S=%d, M=%d, X=%d\n", world_size, stations, max_smell, guard_limit);
        fflush(stdout);
    }

    srand(1234567u + (unsigned int)world_rank);
    smell = 1 + rand_range(0, 4);

    requests = (ProcRequest *)calloc((size_t)world_size, sizeof(ProcRequest));
    if (!requests) {
        fprintf(stderr, "Out of memory while allocating request table\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    for (;;) {
        pump_messages();
        try_progress();
        pump_messages();

        if (banned_count >= world_size) {
            break;
        }
    }

    MPI_Finalize();
    free(requests);
    free(history);
    return 0;
}
