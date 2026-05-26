        # kafka — Kafka Streams: KStream, KTable, топология

        Homework-шаблон для урока **l3_streams_intro** (Kafka Streams: KStream, KTable, топология) на платформе Vibe Learn.

        ## Что делать

        Реализуй word-count топологию через Kafka Streams (или Go-эквивалент через goka).

Задача: читать слова из топика `words-input` (одно слово = одно сообщение), считать количество вхождений каждого слова, записывать актуальный счётчик в топик `word-counts` (ключ = слово, значение = число).

**Для Java/Kotlin (рекомендуется, Streams нативен для JVM):**
- `KStream<Null, String>` из `words-input`
- `mapValues(word -> word.toLowerCase().trim())`
- `groupBy((key, word) -> word)` → `count()` → KTable<String, Long>
- `toStream().to("word-counts")`
- State store: RocksDB (по умолчанию)

**Для Go (goka или sarama-based, если команда на Go-стеке):**
- Использовать goka processor с GroupTable для накопления счётчиков
- Аналогично: читать `words-input`, агрегировать, писать в `word-counts`

**CI-тест (предоставлен в template):**
- Produce 100 известных слов в `words-input` (10 уникальных слов × 10 раз каждое)
- Дождаться обработки (poll output topic до стабилизации)
- Assert: для каждого из 10 слов count == 10 в `word-counts`
- Assert: нет лишних ключей (только 10 уникальных слов)

Примечание: Kafka Streams нативен для JVM — Java/Kotlin рекомендуются для продакшен-применения. Go-template реализует ту же семантику через goka для команд на Go-стеке.

## Контекст (из transfer-задачи урока)

Продакт хочет: «для каждого пользователя считать sliding 24-hour count of failed logins, и если count > 5 — отправить alert в топик `security-alerts`».

Команда уже использует Kafka (топик `login_events`, ключ = user_id, значение = JSON с полем `status`: "success" или "failed").

## Recap из урока

- **Kafka Streams — библиотека, не сервис**: запускается внутри вашего JVM-приложения; никакого отдельного кластера не нужно, только Kafka-брокеры.
- **KStream vs KTable**: KStream = бесконечный поток вставок (каждое сообщение — событие), KTable = материализованный снапшот (только последнее значение на ключ).
- **State stores**: локальный RocksDB на каждом instance, реплицируется в changelog topic — восстанавливается при рестарте автоматически.
- **GlobalKTable** — полная копия таблицы на каждом instance; используется для join с небольшими справочными данными без зависимости от партиционирования.
- **processing.guarantee=exactly_once_v2** включает EOS в Streams (broker 2.5+); под капотом — тот же transactional producer + атомарный commit offset.

        ## Как работать

        1. Платформа Vibe Learn создаёт копию этого репо в твоём GitHub-аккаунте по клику «Начать домашку» на странице урока (через GitHub `/generate`, codecrafters-pattern).
        2. Склонируй копию локально, реализуй TODO в `main.go`, прогони тесты, запушь.
        3. CI (`.github/workflows/ci.yml`) запускает `go vet` + `go test ./...` на каждый push. Платформа слушает результат через webhook от GitHub Actions и обновляет статус домашки на странице урока.

        ## Локальное окружение

        - Go 1.22+
        - Docker + docker-compose — `docker compose -f docker-compose.yml up -d` поднимает 3-нодовый Kafka cluster на портах 9092/9093/9094, использовать в тестах через bootstrap `localhost:9092,localhost:9093,localhost:9094`.

        ## Запуск

        ```bash
        # Поднять локальный Kafka
        docker compose up -d

        # Прогнать тесты (часть из них стартует свой ephemeral testcontainers cluster, часть использует docker-compose выше)
        go test ./...

        # Запустить main (печатает marker; замени stub на реализацию)
        go run .
        ```

        ## Заметка автора

        Это baseline-шаблон, сгенерированный платформой. Бизнес-сущность задачи (что конкретно реализовать в `main.go`, какие тесты сделать строгими) расширяется по ходу итераций — параллельно с углублением теории урока.
