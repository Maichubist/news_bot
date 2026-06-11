# CHANGES

## Фаза 1 — Режим модерації (human-in-the-loop) — ✅ ЗАВЕРШЕНО

### План змін по файлах (виконано)
1. `app/config.py` — нова секція `ModerationCfg` (enabled, timeout_minutes, on_timeout) з дефолтами;
   стара конфігурація без секції `moderation` працює як раніше (moderation вимкнено).
2. `config.yaml` — секція `moderation` (enabled: true, timeout_minutes: 45, on_timeout: skip).
3. `app/storage/sqlite_repo.py` — через `_ensure_extra_columns` (ALTER, ідемпотентно):
   колонки `review_requested_at_utc`, `review_message_id`; таблиця `moderation_log`
   (item_hash, action, decided_at_utc, llm_score, origin, topic_key, category);
   методи mark_pending_review / mark_rejected / get_item_for_moderation /
   get_review_timeouts / add_moderation_log / update_post_text.
   DESIRED_COLS і SCHEMA_V5 не змінювалися — важка міграція не перетригерюється.
4. `app/telegram/client.py` — `reply_markup` у sendMessage/sendPhoto/sendVideo,
   нові методи `answer_callback_query`, `edit_message_reply_markup`.
5. `app/services/publisher.py` (новий) — `ChannelPublisher`: завантаження/відправка медіа
   (винесено з NewsPipeline 1:1, поведінка не змінена), підтримує chat_id/reply_markup —
   модерація публікує і шле прев'ю без залежності від пайплайна.
6. `app/services/moderation.py` (новий) — `ModerationService`:
   - `submit_for_review` — прев'ю (текст + медіа) в admin-чат з inline-клавіатурою
     ✅ Опублікувати / ✏️ Перегенерувати / ❌ Відхилити (callback_data `mod:<action>:<id>`,
     числовий id, бо sha256-хеш не влазить у ліміт 64 байти callback_data);
   - `handle_callback` — переходи статусів: approve → posted (публікація негайно),
     reject → rejected (новий термінальний статус), regen → новий post_text + нове прев'ю,
     статус лишається pending_review, таймер скидається;
   - `check_timeouts` — items старші за timeout_minutes: skip → rejected,
     publish → автопублікація; викликається на початку кожного run_once;
   - перемикач через bot_state (`moderation_enabled`) — пріоритет над config.yaml;
   - кожне рішення → insert у `moderation_log`.
7. `app/services/openai_postmaker.py` — опційний `extra_instruction` (для «перепиши інакше»);
   prompt overrides з БД (`/prompt_set`) діють і при перегенерації (prompt_provider як і раніше).
8. `app/services/news_pipeline.py` — у `_post_pending_roots` дозрілий item при увімкненій
   модерації йде на review замість каналу (зі збереженням «⚡» для breaking у БД, щоб
   прев'ю == опублікованому тексту); `run_once` обробляє таймаути; медіа-методи
   делеговані в ChannelPublisher (рефакторинг без зміни поведінки, крок до Фази 5.3).
9. `app/services/telegram_analytics_commands.py` — обробка callback_query у наявному
   getUpdates-полінгу (без webhooks), тільки від адміна; команда `/moderation on|off`.
10. `app/bootstrap.py` — складання ChannelPublisher + два екземпляри ModerationService
    (окремий repo для потоку команд: sqlite-з'єднання прив'язані до потоку).
11. `tests/` — 30 тестів (pytest): переходи статусів по кожній кнопці, таймаут з обома
    політиками, запис moderation_log, повторний клік не дає подвійної публікації,
    перехоплення публікації пайплайном (on/off), смоук міграцій (свіжа БД + БД
    попередньої версії з даними), reply_markup/answerCallbackQuery у клієнті, конфіг-дефолти.
12. `README.md` — ручний сценарій перевірки модерації. `pytest.ini`, `requirements-dev.txt` (pytest).

### Фіче-флаги / як увімкнути
- `moderation.enabled: true` у config.yaml (дефолт за відсутності секції — false).
- Runtime-перемикач: `/moderation on|off` в admin-чаті (bot_state, без рестарту).
- `moderation.on_timeout: skip|publish` — політика при мовчанні адміна.

### Результат тестів
`python -m pytest` → **30 passed** (запуск 2026-06-10).

### Нотатки
- Нова залежність лише dev-рівня: pytest (вимога ТЗ «тести обов'язкові»); runtime-стек не змінився.
- Статус-машина: new → pending_post → pending_review → posted | rejected | error;
  pending_wrap/digest_only без змін.

### TODO (не блокує фазу)
- `test.py` у корені порожній/мертвий — видалити у Фазі 5.5.
- `app/services/news_pipeline.py:_send_with_optional_photo` — legacy-обгортка, ніде не
  викликається; кандидат на видалення у Фазі 5.5.
- Прев'ю з фото обмежене лімітом caption 1024 символи Telegram: довге прев'ю граційно
  деградує до текстового повідомлення (як і в каналі) — поведінка успадкована, не регресія.

---

## Фаза 2 — Збір engagement-метрик — ✅ ЗАВЕРШЕНО

### План змін по файлах (виконано)
1. `app/config.py` + `config.yaml` — секція `engagement` (enabled, poll_hours: 6,
   lookback_hours: 72, max_posts: 50, mtproto_enabled: false), усі дефолти.
2. `app/storage/sqlite_repo.py` — `_ensure_extra_columns`: колонка `tg_message_id` (ALTER);
   таблиці `post_metrics` (append-only знімки) і `reaction_tally` (акумулятор реакцій
   з message_reaction updates); mark_posted приймає tg_message_id; методи вибірки/запису.
3. `app/telegram/client.py` — параметр `allowed_updates` у get_updates
   (потрібен "message_reaction"; бот має бути адміном каналу).
4. `app/services/engagement.py` (новий) — EngagementService: record_reaction_update
   (дельта-акумуляція), maybe_collect (знімок раз на poll_hours через bot_state),
   опційний MTProto-шлях (Telethon, фіче-флаг + env, без жорсткої залежності),
   build_report_block (топ-5/анти-топ-5, розріз category/topic_key/origin),
   тиха деградація з warning раз на добу.
5. `app/services/news_pipeline.py`, `app/services/moderation.py` — зберігати message_id
   опублікованого поста (publisher.send_media вже повертає msg_id).
6. `app/services/telegram_analytics_commands.py` — роутинг message_reaction updates.
7. `app/services/analytics_service.py` — engagement-блок у щоденному звіті.
8. `app/bootstrap.py` — wiring. `tests/test_engagement.py` — знімки, агрегація звіту,
   graceful degradation без кредів.

### Фіче-флаги / як увімкнути
- `engagement.enabled: true` (дефолт) — реакції збираються, якщо бот адмін каналу;
  знімки post_metrics раз на `poll_hours`.
- `engagement.mtproto_enabled: true` + `pip install telethon` + env
  `TELEGRAM_API_ID`/`TELEGRAM_API_HASH`/`TELEGRAM_MTPROTO_SESSION` — додає views/forwards.
  Без будь-чого з цього модуль працює далі (views/forwards = NULL), warning раз на добу.
- Engagement-блок (топ-5/анти-топ-5, розрізи category/topic_key/origin) автоматично
  додається до щоденного звіту адмінці, коли є хоч один знімок.

### Результат тестів
`python -m pytest` → **43 passed** (запуск 2026-06-10; 13 нових тестів Фази 2).

### Нотатки
- Обмеження Bot API: views/forwards для канальних постів недоступні боту — реалізовано
  реакції через `message_reaction` у наявному getUpdates-полінгу (`allowed_updates`
  передається лише коли engagement увімкнено). Бот ОБОВ'ЯЗКОВО має бути адміном каналу,
  інакше Telegram не шле reaction-updates (тоді знімки міститимуть 0 реакцій).
- `reaction_tally` — акумулятор дельт (old_reaction/new_reaction), `post_metrics` —
  append-only знімки; історія зростання engagement зберігається.
- Виправлено SQL-баг в upsert дельти реакцій (від'ємна дельта губилась через clamp
  у VALUES); покрито тестом `test_reaction_updates_accumulate`.
## Фаза 3 — Батчеве порівняльне ранжування — ✅ ЗАВЕРШЕНО

### План змін по файлах (виконано)
1. `app/config.py` + `config.yaml` — секція `ranking` (enabled: false за замовчуванням,
   cycle_minutes: 25, window_hours: 3, model: gpt-4o, max_picks: 2, max_age_hours: 8).
2. `app/storage/sqlite_repo.py` — колонка `fact_summary` (ALTER), таблиця `ranking_log`
   (cycle_at_utc, candidates_json, picks_json, reasoning); методи: get_ranking_candidates,
   get_expired_candidates, get_recent_posted_topic_keys, save_ranking_log, get_item_full.
3. `app/services/openai_postmaker.py` — спільний `_post_json` хелпер; новий метод
   `classify()` (дешевий перший прохід: класифікація + fact_summary, БЕЗ post_text);
   DEFAULT_CLASSIFY_PROMPT.
4. `app/services/ranker.py` (новий) — RankingLLM (один виклик gpt-4o зі strict JSON,
   retry 1 раз на зламаному JSON) + NewsRanker (цикл кожні cycle_minutes через bot_state:
   збір кандидатів вікна, payload id/title/fact_summary/source/origin/age/source_count,
   контекст origin_share + останні 10 topic_key, переможці → winner_processor,
   fallback на старий поріг при відмові LLM, експірація → wrap/digest, ranking_log).
5. `app/services/news_pipeline.py` — при ranking.enabled: classify → status='candidate'
   (noise/promo → filtered; breaking + ≥2 джерел → негайний повний шлях як зараз);
   `process_ranking_winner()` — повний post_text + editorial ПІСЛЯ-перевірка (вето);
   `_route_expired_candidate()` — wrap/digest із синтезованим post_text (title+fact_summary),
   щоб існуючі wrap/digest-запити працювали. Старий шлях незмінний при enabled: false.
6. `app/services/prompt_manager.py` — ключі classify_prompt, ranking_prompt
   (працюють /prompt_set overrides). `app/bootstrap.py` — wiring (pipeline.ranker).
7. `tests/test_ranker.py`, `tests/test_pipeline_ranking.py` — детермінований мок LLM,
   breaking не чекає циклу, програвші доживають до wrap, e2e run_once з enabled=true/false,
   зламаний JSON → retry → fallback.

### Фіче-флаги / як увімкнути
- `ranking.enabled: true` у config.yaml. За замовчуванням **false** — старий шлях
  (`editorial.min_post_score`) працює без жодних змін; обидва шляхи живуть поруч,
  видалення старого — окреме рішення людини.
- Параметри: `cycle_minutes: 25`, `window_hours: 3`, `model: gpt-4o`, `max_picks: 2`,
  `max_age_hours: 8`.
- Промпти `classify_prompt` і `ranking_prompt` доступні для `/prompt_set` overrides
  (пріоритет над дефолтами в коді, як і решта промптів).

### Як це працює при enabled: true
1. Item після dedup проходить дешевий classify-прохід (та сама модель post_model):
   класифікація + fact_summary, БЕЗ post_text → status='candidate'.
   noise/promo → 'filtered'. is_breaking + ≥2 джерел → негайний повний шлях (як старий).
2. Раз на cycle_minutes NewsRanker одним викликом gpt-4o передає компактний пул
   (id, title, fact_summary, source, origin, вік, source_count) + контекст:
   поточний origin_share (числа) і останні 10 опублікованих topic_key.
3. Переможці → повний post_text сильною моделлю з article_text →
   editorial decide_publish_mode як ПІСЛЯ-перевірка (вето зберігається,
   але абсолютний поріг score до переможців не застосовується) → pending_post →
   існуючий шлях (модерація Фази 1 або публікація).
4. Кандидати, що не виграли за max_age_hours → wrap/digest; post_text синтезується
   з fact_summary (без LLM), бо wrap/digest-вибірки вимагають непорожній post_text.
5. Кожен цикл → рядок у ranking_log (candidates_json, picks_json, reasoning).

### Захист від зламаного JSON
RankingLLM ретраїть рівно 1 раз; після двох невдач цикл переходить на fallback —
старий абсолютний поріг (min_post_score) поверх score з classify-проходу,
top-max_picks за score. Канал не зупиняється.

### Результат тестів
`python -m pytest` → **60 passed** (запуск 2026-06-10; 17 нових тестів Фази 3).

### Нотатки
- `run_once` частково декомпозовано (крок до Фази 5.3): editorial-блок винесено в
  `_decide_and_store()` — спільний для старого шляху і переможців ранкера, поведінка
  старого шляху біт-у-біт збережена (e2e-тест підтверджує).
- callback-цикл pipeline↔ranker розв'язано у bootstrap: ранкер отримує
  `pipeline.process_ranking_winner` / `pipeline._route_expired_candidate` як колбеки.
## Фаза 4 — Кластеризація подій замість LLM event_key — ✅ ЗАВЕРШЕНО

### План змін по файлах (виконано)
1. `app/config.py` + `config.yaml` — секція `clustering` (enabled: false, threshold: 0.80,
   window_hours: 48, wrap: {min_items: 3, min_sources: 2, lookback_hours: 6, cooldown_minutes: 90}).
2. `app/storage/sqlite_repo.py` — колонка `cluster_id` (ALTER), таблиця `event_clusters`
   (cluster_id, centroid_blob, centroid_dim, created_at_utc, canonical_hash, item_count,
   source_count); методи кластерів + вибірки wrap-кластерів.
3. `app/dedup/clusters.py` (новий) — IncrementalClusterer: чистий numpy, новий item
   приєднується до найближчого кластера (cosine ≥ threshold до центроїда, центроїд =
   інкрементальне середнє) або відкриває новий; канонічний представник — найраніший
   item з найповнішим article_text.
4. `app/services/news_pipeline.py` — призначення кластера після embedding;
   `_event_source_count`/`_event_cluster_rows` — рішення по cluster_id при
   clustering.enabled (event_key лишається в БД як довідкове); wraps по кластерах:
   `_process_cluster_wraps` (хронологія кластера → промпт «розвиток сюжету»),
   старі wrap_rules по категоріях працюють без змін при enabled: false.
5. `app/services/ranker.py` — ін'єкція source_count_fn (cluster-aware при увімкненому флагу).
6. `app/services/prompt_manager.py` + bootstrap — ключ cluster_wrap_prompt (override-able),
   дефолти classify/ranking/cluster_wrap зареєстровані в PromptManager.
7. `tests/test_clusters.py` — приєднання/новий кластер/оновлення центроїда/канонічний;
   міграція event_key→cluster_id логіки; wrap-по-кластеру з фейковим LLM.

### Фіче-флаги / як увімкнути
- `clustering.enabled: true`. За замовчуванням **false** — event_key-матчинг і
  category-wraps працюють без жодних змін.
- `clustering.threshold: 0.80` — cosine до центроїда; `window_hours: 48` — активне вікно.
- `clustering.wrap` — пороги сюжетних wrap'ів (min_items/min_sources/lookback/cooldown).
- Промпт `cluster_wrap_prompt` («розвиток сюжету», отримує хронологію кластера) —
  override через `/prompt_set cluster_wrap_prompt …`.

### Що змінюється при enabled: true
- Кожен item з embedding (включно з дублями — вони підтверджують подію іншим джерелом)
  призначається кластеру: cosine ≥ threshold до центроїда → приєднання (центроїд —
  інкрементальне середнє), інакше — новий кластер. Канонічний представник —
  найраніший item з найповнішим article_text (переобирається при кожному приєднанні).
- Рішення, що читали event_key (`source_count` для breaking-підтвердження у пайплайні
  й ранкері, cluster_rows для new-fact gate) — ідуть через cluster_id
  (`_event_source_count` / `_event_cluster_rows`); event_key лишається в БД як довідка.
- Wraps: одиниця — кластер ≥ min_items items від ≥ min_sources джерел за lookback;
  промпт отримує хронологію (titles + fact_summary за часом) і пише «розвиток сюжету».
  Cooldown per-cluster через market_wrap_posts (wrap_name=`cluster:<id>`).

### Результат тестів
`python -m pytest` → **70 passed** (запуск 2026-06-10; 10 нових тестів Фази 4).

### Нотатки
- event_key з LLM більше НЕ використовується для матчингу при clustering.enabled —
  лишився довідковим полем (DESIRED_COLS/SCHEMA_V5 не чіпались).
- Перевага кластерів покрита тестом: два джерела дали РІЗНІ event_key для однієї події —
  старий шлях бачить 1 джерело, кластер бачить 2.
## Фаза 5 — Інженерна гігієна — ✅ ЗАВЕРШЕНО

### Виконано
1. Retry error-items: колонки retry_count/next_retry_utc (ALTER); до 3 спроб з
   експоненційною паузою (10/20/40 хв); після 3 — status='failed' (термінальний) + алерт
   в admin-чат. Крок `_retry_errors` у run_once.
2. Funnel-метрики: таблиця funnel_daily(day_utc, stage, count); лічильники за цикл,
   один рядок логу наприкінці run_once; команда `/funnel` (суми за сьогодні/вчора).
3. Декомпозиція run_once: `_ingest()`, `_match_and_dedup()`, `_score_or_collect()`,
   `_decide()` (= наявний `_decide_and_store`), `_publish_due()` — поведінка без змін
   (покрито наявними e2e-тестами). Раніше вже винесено: ChannelPublisher, _decide_and_store.
4. Embed-відео: у articles/extract.py детект YouTube/Vimeo (og:video, twitter:player,
   iframe-embed) БЕЗ завантаження; колонка embed_video_url (ALTER); рядок
   «▶️ Відео: <лінк>» після тексту поста. yt-dlp не використовується.
5. Мертвий код: видалено app/translate/ (enabled: false, жодного імпорту; TranslateCfg
   лишився в config.py для зворотної сумісності старих config.yaml), порожній test.py,
   legacy `_send_with_optional_photo`.
6. Makefile (make test) + scripts/test.ps1; смоук міграцій уже в tests/test_migrations.py.
7. README.md: текстова схема пайплайна, опис усіх секцій config.yaml, запуск,
   фіче-флаги, validate_sources, тести.

### Результат тестів
`python -m pytest` → **81 passed** (запуск 2026-06-11; 11 нових тестів Фази 5:
retry/backoff/failed+алерт, funnel-акумуляція/запис із run_once/команда /funnel,
embed-відео детект (og:video youtube, iframe vimeo, не-embed ігнорується),
рядок «▶️ Відео» у форматері).

### Нотатки
- Декомпозиція run_once не змінила поведінку: усі 70 тестів попередніх фаз
  пройшли без правок одразу після рефакторингу.
- `test.py` містив 372 символи (не порожній), видалений згідно з ТЗ —
  відновлюваний з git-історії.
- `TranslateCfg` лишився в config.py: секція `translate` обов'язкова у старих
  config.yaml (зворотна сумісність), сам модуль app/translate/ видалено.

---

## TODO (знайдено під час роботи, не блокувало фази)
- `app/dedup/semantic.py:find_best_match` передає `window_hours` (int) у
  `repo.get_recent_embeddings(since_iso=...)` — порівняння TEXT >= INTEGER у SQLite
  завжди true, тож вікно фактично не обмежує вибірку (працює, але повільніше,
  ніж задумано). Виправлення: передавати ISO-час `(now - window_hours)`.
- Прев'ю модерації з фото обмежене лімітом caption 1024 символи Telegram —
  довгі прев'ю граційно деградують до тексту (успадкована поведінка каналу).
- `/funnel` показує доби UTC, а не ковзні 24h — прийнятне наближення,
  зафіксовано в довідці команди.

## Підсумок усіх фаз
- Фаза 1 (модерація): ✅, 30 тестів. Фаза 2 (engagement): ✅, +13.
- Фаза 3 (ранжування): ✅, +17. Фаза 4 (кластеризація): ✅, +10. Фаза 5: ✅, +11.
- Разом: **81 тест зелений**. Нові фіче-флаги: moderation.enabled,
  engagement.enabled/mtproto_enabled, ranking.enabled, clustering.enabled —
  усі з дефолтами, стара конфігурація працює без правок.
- Нова залежність: лише pytest (dev). Runtime-стек незмінний:
  requests, sqlite3, feedparser, trafilatura, PyYAML, numpy.
