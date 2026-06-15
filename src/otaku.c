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
} RequestId;    //identyfikator zdarzenia w logice Lamporta / timestamp

typedef struct {
    int ts;
    int pid;
    int smell;
} Entry;        //wpis historii wejść do sekcji krytycznej

typedef struct {
    int ts;
    int smell;
    bool active;
    bool inside;
} ProcRequest;  //stan jednego procesu w tablicy symulacji

int n_count = 0;        //liczba otaku
int pid = 0;            //identyfikator procesu MPI 
int lamport = 0;        //zegar lamporta

int stations = 0;       //liczba stanowisk
int max_smell = 0;      //max smell w pokoju / przed banem
int guard_limit = 0;    //wartość X - dawna przed FAINTem strażnika

int smell = 1;          //aktualny smród procesu/otaku

ProcRequest *requests = NULL;   //tablica stanow wszystkich procesow
Entry *history = NULL;          //historia wejść do sekcji krytycznej 
int history_count = 0;          //liczba wpisów w historii
int history_cap = 0;            //aktualna pojemnosc bufora historii
long long history_sum = 0;      //suma smrodu z aktualnej historii

long long dose = 0;                 //aktualna dawka x_acc
RequestId cut_id = {-1, -1};        //odcięcie historii po ostatnim faincie
RequestId my_request_id = {-1, -1}; //identyfikator obecnego zgłoszenia procesu

bool want_enter = false;    //czy proces chce wejść do sekcji krytycznej
bool inside = false;        //czy proces jest teraz w sekcji krytycznej (w pokoju)
bool faint_sent = false;    //czy proces juz wysłał FAINT dla tej rundy
bool excluded = false;      //czy proces został zbanowany
bool slow = true;           //tryb wolniejszej symulacji
int ack_count = 0;          //ile ACK juz przyszło
int work_steps_left = 0;    //ile krokow pracy zostało w sekcji krytycznej (symulacja siedzenia przy stanowisku)
int banned_count = 0;       //ile procesow dostało bana

// Wypisuje log z numerem procesu i czasem Lamporta.
void log_msg(const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    printf("[%d] [t%d] ", pid + 1, lamport);
    vprintf(fmt, ap);
    printf("\n");
    fflush(stdout);
    va_end(ap);
}

// Porównuje dwa identyfikatory zadań po ts i pid
int id_cmp(RequestId a, RequestId b) {
    if (a.ts != b.ts) {
        return (a.ts < b.ts) ? -1 : 1;
    }
    if (a.pid != b.pid) {
        return (a.pid < b.pid) ? -1 : 1;
    }
    return 0;
}

// Zwiększa zegar przed wysłaniem wiadomości.
void clock_send_tick(void) {
    lamport++;
}

// Aktualizuje zegar po odebraniu wiadomości.
void clock_recv_tick(int received_ts) {
    if (lamport < received_ts) {
        lamport = received_ts;
    }
    lamport++;
}

// Losowa liczba z zakresu domkniętego
int rand_range(int min_value, int max_value) {
    return min_value + rand() % (max_value - min_value + 1);
}

// Aktualny smród w pokoju.
long long current_room_smell(void) {
    long long room_smell = 0;
    for (int i = 0; i < n_count; ++i) {
        if (requests[i].inside) {
            room_smell += requests[i].smell;
        }
    }
    return room_smell;
}

// Krótka przerwa jak tryb slow jest włączony
void sleep_cycle_delay(void) {
    if (!slow) {
        return;
    }

    double start = MPI_Wtime();
    double delay = (double)rand_range(1, 3);
    while (MPI_Wtime() - start < delay) {
        // aktywne czekanie dla poprawienia czytelności
    }
}

// Powieksza bufor historii wejść, gdy jest pełny.
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

// Wstawia nowe wejscie do historii w kolejności czasowej
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

// Usuwa z historii wpisy starsze lub równe od danego timestampu+pid
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

// Usuwa z historii pierwszy wpis danego procesu.
void remove_history_pid(int target_pid) {
    for (int i = 0; i < history_count; ++i) {
        if (history[i].pid != target_pid) {
            continue;
        }

        history_sum -= history[i].smell;
        if (i + 1 < history_count) {
            memmove(history + i, history + i + 1, (size_t)(history_count - i - 1) * sizeof(Entry));
        }
        history_count--;
        return;
    }
}

// Pakuje i wysyla jedna wiadomosc MPI.
void pack_send(int dest, int tag, int a, int b, int c) {
    int msg[4] = {a, b, c, 0};
    MPI_Send(msg, 4, MPI_INT, dest, tag, MPI_COMM_WORLD);
}

// Rozsyla wiadomosc do wszystkich pozostalych procesow.
void broadcast(int tag, int a, int b, int c) {
    for (int dest = 0; dest < n_count; ++dest) {
        if (dest == pid) {
            continue;
        }
        pack_send(dest, tag, a, b, c);
    }
}

// Rozsyła informacja o banie do wszystkich procesow
void broadcast_banned(void) {
    for (int dest = 0; dest < n_count; ++dest) {
        if (dest == pid) {
            continue;
        }
        pack_send(dest, TAG_BANNED, pid, 0, 0);
    }
}

// Porownuje aktywne procesy po ich czasie zgloszenia.
int active_request_compare(const void *lhs, const void *rhs) {
    int a = *(const int *)lhs;
    int b = *(const int *)rhs;
    RequestId ia = {(int)requests[a].ts, a};
    RequestId ib = {(int)requests[b].ts, b};
    return id_cmp(ia, ib);
}

// Buduje posortowaną liste aktywnych procesow.
int build_active_list(int *active_pids) {
    int count = 0;
    for (int proc_pid = 0; proc_pid < n_count; ++proc_pid) {
        if (requests[proc_pid].active) {
            active_pids[count++] = proc_pid;
        }
    }
    qsort(active_pids, (size_t)count, sizeof(int), active_request_compare);
    return count;
}

// Sprawdza, czy ten proces moze wejsc do sekcji krytycznej.
bool self_is_allowed(void) {
    int *active_pids = (int *)malloc((size_t)n_count * sizeof(int));
    if (!active_pids) {
        fprintf(stderr, "Out of memory while building active request list\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    int active_count = build_active_list(active_pids);
    int admitted = 0;
    long long smell_sum = 0;
    bool allowed = false;

    for (int i = 0; i < active_count; ++i) {
        int proc_pid = active_pids[i];
        if (admitted < stations && smell_sum + requests[proc_pid].smell <= max_smell) {
            if (proc_pid == pid) {
                allowed = true;
                break;
            }
            admitted++;
            smell_sum += requests[proc_pid].smell;
        } else {
            break;
        }
    }

    free(active_pids);
    return allowed;
}

// Aktualizuje stan po faincie.
void apply_faint(RequestId trigger) {
    dose -= guard_limit;
    if (dose < 0) {
        dose = 0;
    }
    trim_history_prefix(trigger);
    cut_id = trigger;
}

// Szuka wpisu, ktory przekracza próg X by ustalić winowajce aby on wysłał faint. 
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

// Wysyla FAINT, gdy ten proces sam wywolal przekroczenie progu.
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

    faint_sent = true;
    log_msg("Wysyłam FAINT dla wejścia (%d,%d), accumulated=%lld", trigger.ts, trigger.pid + 1, accumulated);
    apply_faint(trigger);
    broadcast(TAG_FAINT, trigger.ts, trigger.pid, 0);
}

// Odbiera REQUEST i odsyla ACK do nadawcy.
void handle_request_message(int ts, int proc_pid, int smell_value) {
    clock_recv_tick(ts);
    requests[proc_pid].ts = ts;
    requests[proc_pid].smell = smell_value;
    requests[proc_pid].active = true;
    if (proc_pid == pid) {
        return;
    }
    pack_send(proc_pid, TAG_ACK, lamport, pid, 0);
}

// Odbiera ENTER i zapisuje wejscie do historii.
void handle_enter_message(int ts, int proc_pid, int smell_value) {
    clock_recv_tick(ts);
    RequestId entry_id = {ts, proc_pid};
    if (id_cmp(entry_id, cut_id) <= 0) {
        return;
    }
    if (!requests[proc_pid].active || requests[proc_pid].ts != ts) {
        return;
    }

    requests[proc_pid].inside = true;
    insert_history((Entry){ts, proc_pid, smell_value});
    dose += smell_value;
    maybe_send_faint();
}

// Odbiera RELEASE i usuwa proces z historii
void handle_release_message(int ts, int proc_pid) {
    clock_recv_tick(ts);
    if (!requests[proc_pid].active || requests[proc_pid].ts != ts) {
        return;
    }
    remove_history_pid(proc_pid);
    requests[proc_pid].inside = false;
    requests[proc_pid].active = false;
}

// Odbiera FAINT i naklada odciecie na biezacy stan.
void handle_faint_message(int trigger_ts, int trigger_pid) {
    clock_recv_tick(trigger_ts);
    RequestId trigger = {trigger_ts, trigger_pid};
    apply_faint(trigger);
}

// Liczy ban dla procesu, ktory dostal info o wykluczeniu.
void handle_banned_message(int source_pid) {
    (void)source_pid;
    banned_count++;
}

// Przetwarza wszystkie oczekujace wiadomosci MPI.
void pump_messages(void) {
    int flag = 0;
    MPI_Status status;

    while (1) {
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
                    if (ack_count == n_count - 1) {
                        log_msg("Odebrałem wszystkie ACK (%d/%d)", ack_count, n_count - 1);
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
                handle_faint_message(msg[0], msg[1]);
                break;
            case TAG_BANNED:
                handle_banned_message(msg[0]);
                break;
            default:
                break;
        }
    }
}

// Rozpoczyna probe wejscia do sekcji krytycznej.
void start_request(void) {
    clock_send_tick();
    my_request_id.ts = lamport;
    my_request_id.pid = pid;
    want_enter = true;
    faint_sent = false;
    ack_count = 0;

    requests[pid].ts = my_request_id.ts;
    requests[pid].smell = smell;
    requests[pid].active = true;
    requests[pid].inside = false;

    log_msg("Rozpoczynam staranie o sekcję krytyczną (smród=%d, x_acc=%lld, smród_w_sali=%lld)", smell, dose, current_room_smell());
    broadcast(TAG_REQUEST, my_request_id.ts, pid, smell);
}

// Wchodzi do sekcji krytycznej i rozglasza ENTER.
void enter_room(void) {
    clock_send_tick();
    inside = true;
    requests[pid].inside = true;

    insert_history((Entry){my_request_id.ts, pid, smell});
    dose += smell;
    log_msg("Wchodzę do sekcji krytycznej (x_acc=%lld, smród_w_sali=%lld, smród=%d)", dose, current_room_smell(), smell);

    broadcast(TAG_ENTER, my_request_id.ts, pid, smell);
    maybe_send_faint();

    // Liczba krokow losowa żeby symulacja byla mniej równa.
    work_steps_left = 3 + rand_range(0, 4);
}

// Wychodzi z sekcji krytycznej i aktualizuje stan po wyjsciu.
void release_room(void) {
    clock_send_tick();

    broadcast(TAG_RELEASE, my_request_id.ts, pid, 0);

    inside = false;
    want_enter = false;
    requests[pid].active = false;
    requests[pid].inside = false;
    work_steps_left = 0;

    log_msg("Wychodzę z sekcji krytycznej (x_acc=%lld, smród_w_sali=%lld, smród=%d)", dose, current_room_smell(), smell);

    smell += rand_range(2, 5);
    if (smell > max_smell) {
        excluded = true;
        log_msg("Otaku %d został zbanowany z Pyrconu, aktualny smród=%d", pid + 1, smell);
        banned_count++;
        broadcast_banned();
    }
}

// Sprawdza, czy mozna przejsc do kolejnego kroku symulacji.
void try_progress(void) {
    if (excluded) { //zbanowany to nie rób nic
        return;
    }

    if (!want_enter && !inside) { //jak nie stara się jeszcze o wejście, to zacznij
        start_request();
    }

    if (want_enter && !inside && ack_count >= n_count - 1) { //chce wejść i ma wszystkie ACK, sprawdź czy może wejść
        if (self_is_allowed()) {
            log_msg("Mam komplet ACK, wysyłam ENTER (x_acc=%lld, smród_w_sali=%lld, smród=%d)", dose, current_room_smell(), smell);
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

// Parsuje liczbę z argumentu
int parse_int(const char *text, const char *name) {
    char *end = NULL;
    long value = strtol(text, &end, 10);
    if (!text[0] || (end && *end != '\0')) {
        fprintf(stderr, "Niepoprawna wartość parametru %s: %s\n", name, text);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    if (value < 0 || value > 100000L) {
        fprintf(stderr, "Wartość parametru %s poza zakresem: %s\n", name, text);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    return (int)value;
}

// Main (inicjalizacja MPI, parsowanie argumentów, główna pętla)
int main(int argc, char **argv) {
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &pid);
    MPI_Comm_size(MPI_COMM_WORLD, &n_count);

    if (argc < 4) {
        if (pid == 0) {
            fprintf(stderr, "Użycie: %s S M X [slow]\n", argv[0]);
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

    if (guard_limit < max_smell) {
        fprintf(stderr, "X nie może być mniejsze od M.\n");
        MPI_Finalize();
        return 1;
    }

    if (stations <= 0 || guard_limit <= 0) {
        if (pid == 0) {
            fprintf(stderr, "S i X muszą być dodatnie.\n");
        }
        MPI_Finalize();
        return 1;
    }

    if (pid == 0) { // tylko proces 0 wypisuje parametry (żeby wszstkie nie wypisały tego samego)
        printf("Start: N=%d, S=%d, M=%d, X=%d\n", n_count, stations, max_smell, guard_limit);
        fflush(stdout);
    }

    //seed dla rng
    srand(1729u + (unsigned int)pid);
    smell = 1 + rand_range(0, 4);

    requests = (ProcRequest *)calloc((size_t)n_count, sizeof(ProcRequest));
    if (!requests) {
        fprintf(stderr, "Out of memory while allocating request table\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    // GŁÓWNA PĘTLA SYMULACJI
    while (1) {
        sleep_cycle_delay();
        pump_messages();
        try_progress();
        pump_messages();

    // jak wszyscy są zbanowani to exit
    if (banned_count >= n_count) {
            break;
        }
    }

    MPI_Finalize();
    free(requests);
    free(history);
    return 0;
}
