# План по интеграции CacheDB в класс Database

Этот документ описывает шаги по переносу функциональности синхронного кеширования из `CacheDB` (используемого в `simple_scraper.py`) в асинхронный класс `Database` (`database.py`), с последующим рефакторингом и удалением устаревшего кода.

## 1. Анализ методов для переноса

На основе анализа `simple_scraper.py` были определены следующие методы `CacheDB`, которые необходимо реализовать или адаптировать в классе `Database`:

| Метод в `CacheDB` (предполагаемый) | Аналог в `Database` (целевой) | Назначение |
| --- | --- | --- |
| `__init__(db_path)` | `__init__(db_path)` | Инициализация пути к БД. |
| `is_fresh(slug, ttl_days)` | `is_film_cache_fresh(slug, ttl_days)` | Проверка актуальности кеша для фильма. |
| `get_film_row(slug)` | `get_film_cache(slug)` | Получение полной строки данных о фильме. |
| `get_session(slug)` | `get_session_cache(slug)` | Получение даты ближайшего сеанса. |
| `upsert_film(...)` | `upsert_film_cache(...)` | Вставка или обновление полной информации о фильме. |
| `upsert_session(slug, next_date)` | `upsert_session_cache(slug, next_date)` | Вставка или обновление информации о сеансе. |

## 2. Адаптация кода и рефакторинг `Database`

### 2.1. Обновление схемы БД

Необходимо расширить схему таблицы `films` в методе `_ensure_schema` класса `Database` в файле [`movie_scraper/database.py`](movie_scraper/database.py:80), добавив поля из миграции [`movie_scraper/patches/migration.py`](movie_scraper/patches/migration.py:1).

**Инструкция:**
Замените существующий `CREATE TABLE IF NOT EXISTS films` следующим кодом:

```sql
-- Films table with comprehensive metadata and enrichment fields
CREATE TABLE IF NOT EXISTS films (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    slug TEXT UNIQUE NOT NULL,
    title TEXT NOT NULL,
    country TEXT,
    rating TEXT,
    description TEXT,
    poster_url TEXT,
    age_limit TEXT,
    source_url TEXT,
    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Enrichment fields
    imdb_rating REAL,
    kp_rating REAL,
    trailer_url TEXT,
    year INTEGER
);
```

### 2.2. Рефакторинг методов кеширования

Текущие "cache" методы в `Database` являются временным решением. Их следует сделать основными методами для работы с данными, полностью заменив логику `CacheDB`.

**Инструкции:**
1.  **`upsert_film_cache`**: Этот метод должен стать основным способом сохранения данных. Его реализация с использованием `REPLACE INTO` является корректной для логики скрейпера. Следует убедиться, что он принимает все новые поля схемы.
2.  **`get_film_cache`**: Метод должен запрашивать все поля, включая новые (`imdb_rating`, `kp_rating`, `trailer_url`, `poster_url`, `year`).
3.  **`upsert_session_cache`**: Логика с созданием "dummy" сеанса (`cached_cinema`) является обходным путем. Вместо этого, при вызове с `next_date`, следует создавать запись в `screening_sessions` с минимально необходимыми данными. Если `next_date` равен `None`, записи следует удалять. Это сохранит совместимость, но сделает поведение более предсказуемым.

## 3. Файлы для модификации

### 3.1. `movie_scraper/database.py`
-   **Обновить схему:** Внести изменения в `_ensure_schema` согласно п. 2.1.
-   **Проверить методы:** Убедиться, что `upsert_film_cache` и `get_film_cache` работают с новыми полями.

### 3.2. `movie_scraper/simple_scraper.py`
-   **Заменить `CacheDB` на `Database`:**
    -   Удалить упоминание `CacheDB` и импорт `ensure_enrichment_columns`.
    -   Импортировать `Database`.
    -   Заменить инициализацию `db = CacheDB(CACHE_DB)` на асинхронную:
        ```python
        db = Database(CACHE_DB_PATH) # Используется новое имя переменной
        # await db.initialize() должен быть вызван в основной async функции
        ```
-   **Сделать вызовы асинхронными:**
    -   Заменить все вызовы методов `db.*` на `await db.*`.
    -   Пример: `db.is_fresh(f.slug, ...)` -> `await db.is_film_cache_fresh(f.slug, ...)`.
    -   Пример: `db.upsert_film(...)` -> `await db.upsert_film_cache(...)`.

### 3.3. `movie_scraper/patches/migration.py`
-   Этот файл становится ненужным после обновления схемы в `Database`. Его следует **удалить**.

## 4. Финальная структура `Database`

После рефакторинга класс `Database` будет единственным слоем для работы с базой данных. Его ключевые публичные методы будут выглядеть так:

-   `async def initialize()`
-   `async def close()`
-   `async def upsert_film_cache(...)`
-   `async def get_film_cache(slug: str)`
-   `async def get_session_cache(slug: str)`
-   `async def upsert_session_cache(slug: str, next_date: Optional[date])`
-   `async def is_film_cache_fresh(slug: str, ttl_days: int)`

Эта структура полностью покроет потребности скрейпера в кешировании и хранении данных, устранит дублирование и сделает код более чистым и предсказуемым.