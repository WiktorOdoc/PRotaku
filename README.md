# Algorytm rozproszony - Otaku

## Parametry

* N - liczba otaku
* S - liczba stanowisk
* M - maksymalny chwilowy smród w sali
* X - maksymalna dawka smrodu którą może przyjąć strażnik

Po każdym opuszczeniu sali smród danego otaku rośnie o losową wartość.

Jeżeli smród otaku przekroczy M, zostaje on wykluczony z dalszego udziału w algorytmie.

---


Właściwość kluczowa:

* dokładnie jeden proces wysyła komunikat `FAINT` dla danego omdlenia strażnika,
* pozostałe procesy tylko aktualizują swój lokalny stan po odebraniu `FAINT`,
* każdy proces odejmuje X od lokalnego `X_acc` tylko raz dla danego omdlenia.

Komunikat `BANNED` -  proces ogłasza, że został wykluczony z Pyrconu.
Gdy wszyscy otaku zostaną wykluczeni, symulacja kończy się.

---

## Stan lokalny

Każdy proces przechowuje:

* zegar Lamporta
* własny smród
* kolejkę `Q` z odebranymi, jeszcze nie usuniętymi `REQUEST`
* zbiór procesów aktualnie obecnych w sali `inside_set`
* identyfikator ostatniego obsłużonego omdlenia `last_faint_id`
* identyfikator ostatniego wejścia, które zostało już rozliczone przez strażnika `cut_id`
* lokalną wartość skumulowanego smrodu `X_acc`
* licznik odebranych potwierdzeń `ack_count`
* flagę `want_enter`, oznaczającą, że proces czeka na wejście
* flagę `faint_sent`, oznaczającą, że dla aktualnego omdlenia został już wysłany `FAINT`
* lokalny licznik kolejnych omdleń `next_faint_seq`
* licznik odebranych komunikatów `BANNED`

Wszystkie procesy traktują kolejkę `Q` jako wspólny, deterministyczny porządek żądań dostępu, posortowany po `(timestamp Lamporta, pid)`.

---

## Typy wiadomości

### REQUEST(ts, pid, smell)

Żądanie dostępu do sali. Zawiera:

* `ts` - znacznik Lamporta nadawcy
* `pid` - identyfikator procesu
* `smell` - smród procesu

### ACK(ts, pid)

Potwierdzenie odebrania `REQUEST`.

### ENTER(ts, pid, smell)

Informacja, że procesowi faktycznie udało się wejść do sali.

### RELEASE(ts, pid)

Informacja, że proces opuścił salę.

### FAINT(trigger_ts, trigger_pid, faint_id)

Informacja, że strażnik zemdlał po przekroczeniu dawki X.

* `trigger_ts`, `trigger_pid` - identyfikator wejścia, które wywołało omdlenie
* `faint_id` - identyfikator tego konkretnego omdlenia, używany do ochrony przed duplikatami

### BANNED(pid)

Informacja, że proces `pid` został wykluczony z Pyrconu.

---

## Stan: spoczynek

Proces nie prosi jeszcze o wejście.

### Odbiór `REQUEST(ts, pid, smell)`

1. wstaw wpis do `Q`
2. wyślij `ACK`

### Odbiór `ACK`

1. zignoruj

### Odbiór `ENTER(ts, pid, smell)`

1. jeżeli `(ts, pid) <= cut_id`, zignoruj wpis jako spóźniony
2. w przeciwnym razie dodaj proces do `inside_set`

### Odbiór `RELEASE(ts, pid)`

1. jeżeli `(ts, pid) <= cut_id`, zignoruj wpis jako spóźniony
2. w przeciwnym razie usuń proces z `inside_set`

### Odbiór `BANNED(pid)`

1. zwiększ licznik odebranych komunikatów `BANNED`
2. odnotuj, że proces `pid` został wykluczony z Pyrconu

### Odbiór `FAINT(trigger_ts, trigger_pid, faint_id)`

1. jeżeli `faint_id <= last_faint_id`, zignoruj wiadomość
2. w przeciwnym razie:
   * ustaw `last_faint_id = faint_id`
   * odejmij `X` od `X_acc`
   * usuń z `Q` wszystkie wpisy o identyfikatorze `<= (trigger_ts, trigger_pid)`
   * ustaw `cut_id = (trigger_ts, trigger_pid)`
   * ponownie przelicz aktualny `X_acc` z pozostałego sufiksu kolejki

---

## Stan: oczekiwanie na wejście

Proces rozgłosił już `REQUEST` i czeka na komplet `ACK`.

### Wejście do stanu

Proces:

1. zwiększa zegar Lamporta
2. zapisuje swój identyfikator wejścia `my_request_id = (ts, pid)`
3. ustawia `want_enter = true`
4. ustawia `faint_sent = false`
5. dodaje swój wpis do `Q`
6. wysyła `REQUEST(ts, pid, smell)` do wszystkich pozostałych procesów

### Odbiór `REQUEST(ts, pid, smell)`

1. wstaw wpis do `Q`
2. wyślij `ACK(ts_local, my_pid)`

### Odbiór `ACK`

1. zwiększ `ack_count`
2. jeżeli `ack_count == N - 1`, przejdź do wyznaczania prawa wejścia

### Odbiór `ENTER`

1. jeżeli `(ts, pid) <= cut_id`, zignoruj wpis jako spóźniony
2. w przeciwnym razie dodaj proces do `inside_set`

### Odbiór `RELEASE`

1. jeżeli `(ts, pid) <= cut_id`, zignoruj wpis jako spóźniony
2. w przeciwnym razie usuń proces z `inside_set`

### Odbiór `BANNED(pid)`

1. zwiększ licznik odebranych komunikatów `BANNED`
2. odnotuj, że proces `pid` został wykluczony z Pyrconu

### Odbiór `FAINT(trigger_ts, trigger_pid, faint_id)`

1. zastosuj reguły zebrane w stanie spoczynku

### Wyznaczanie prawa wejścia

Po otrzymaniu wszystkich `ACK` proces:

1. sortuje `Q` po `(ts, pid)`
2. bierze odcinek kolejki od `cut_id` do końca
3. wyznacza skumulowany smród `X_acc` dla tego odcinka
4. znajduje pierwszy wpis, po którym suma przekracza `X`

Jeżeli pierwszy taki wpis to własne `my_request_id`, proces jest jedynym nadawcą `FAINT` dla tego omdlenia.

Wtedy:

1. wysyła `ENTER(my_request_id.ts, pid, smell)` do wszystkich
2. jeżeli `faint_sent == false`, wysyła `FAINT(my_request_id.ts, pid, next_faint_seq)` do wszystkich
3. ustawia `faint_sent = true`
4. zwiększa `next_faint_seq`

Jeżeli pierwszy taki wpis nie jest własnym wpisem, proces:

1. czeka na dalsze `ENTER` i `FAINT`
2. nie wysyła `FAINT`

---

## Stan: w sali

Proces znajduje się już w sali i korzysta ze stanowiska.

### Wejście do stanu

Proces po odebraniu uprawnienia do wejścia:

1. wysyła `ENTER`
2. dodaje siebie do `inside_set`
3. wypisuje komunikat o wejściu do sekcji

### Odbiór `REQUEST(ts, pid, smell)`

1. wstaw wpis do `Q`
2. wyślij `ACK`

### Odbiór `ENTER(ts, pid, smell)`

1. jeżeli `(ts, pid) <= cut_id`, zignoruj wpis jako spóźniony
2. w przeciwnym razie dodaj proces do `inside_set`

### Odbiór `RELEASE(ts, pid)`

1. jeżeli `(ts, pid) <= cut_id`, zignoruj wpis jako spóźniony
2. w przeciwnym razie usuń proces z `inside_set` i usuń jego wpis z `Q`

### Odbiór `BANNED(pid)`

1. zwiększ licznik odebranych komunikatów `BANNED`
2. odnotuj, że proces `pid` został wykluczony z Pyrconu

### Odbiór `FAINT(trigger_ts, trigger_pid, faint_id)`

1. jeżeli `faint_id <= last_faint_id`, zignoruj wiadomość
2. w przeciwnym razie:
   * ustaw `last_faint_id = faint_id`
   * odejmij `X` od `X_acc`
   * usuń z `Q` wszystkie wpisy do `(trigger_ts, trigger_pid)` włącznie
   * ustaw `cut_id = (trigger_ts, trigger_pid)`
   * przelicz `X_acc` na podstawie pozostałego sufiksu `Q`
   * jeżeli po odjęciu nadal `X_acc > X`, wyznacz nowy trigger i, jeśli jestem jego właścicielem, wyślij kolejny `FAINT`

### Zakończony pobyt w sali

Po zakończeniu korzystania ze stanowiska proces:

1. wysyła `RELEASE`
2. usuwa siebie z `inside_set`
3. zwiększa swój smród o losową wartość

Jeżeli po aktualizacji smród przekroczy M, proces zostaje wykluczony, wysyła `BANNED` do pozostałych procesów i nie wysyła już kolejnych `REQUEST`.

---

## Stan: obsługa omdlenia

Stan ten jest logiczny, a nie osobna faza komunikacji. Procesy wchodzą do niego po odebraniu `FAINT`.

### Reguły obsługi `FAINT`

1. Każdy proces pamięta `last_faint_id`.
2. Jeżeli odebrany `FAINT` ma identyfikator nie większy niż `last_faint_id`, wiadomość jest duplikatem i należy ją zignorować.
3. W przeciwnym razie proces:
   * odejmuje `X` od lokalnego `X_acc`
   * usuwa z kolejki tylko prefix do `(trigger_ts, trigger_pid)` włącznie
   * ustawia nową granicę `cut_id`
   * przelicza `X_acc` tylko na pozostałej części kolejki

To gwarantuje, że:

* każde omdlenie jest rozliczane tylko raz,
* spóźnione wiadomości nie przywrócą stanu sprzed omdlenia,
* pamięć zużywana przez historię pozostaje ograniczona.

---

## Właściwości algorytmu

* brak centralnego zarządcy
* wszystkie procesy mają równorzędną rolę
* porządek wejść jest deterministyczny dzięki znacznikom Lamporta
* dokładnie jeden proces wysyła `FAINT` dla danego omdlenia
* obsługa `FAINT` jest idempotentna
* procesy nie są głodzone, bo kolejka jest wyznaczana według stałego porządku `(ts, pid)`
* pamięć jest ograniczana przez usuwanie prefixu historii po każdym omdleniu
* po wykluczeniu wszystkich otaku symulacja się kończy
