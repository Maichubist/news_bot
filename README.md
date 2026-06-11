# news_bot

Telegram-бот українського новинного каналу. Парсить RSS (українські + світові
джерела), дедуплікує, оцінює через LLM, застосовує editorial policy і публікує
в канал — з опційними режимами модерації, порівняльного ранжування та
кластеризації подій.

## Архітектура пайплайна

```
RSS-фіди (sources, origin: ua|world)
   │  app/rss/fetcher.py — фетч + глобальні deny-фільтри + медіа з фідів
   ▼
_ingest                exact dedup (sha256 title+link) → upsert у news_items
   ▼
_match_and_dedup       text profile → embedding (OpenAI) → semantic dedup
   │                   → lexical/event matching → dup_of / same_event_of
   │                   → [clustering.enabled] кластер подій (cosine до центроїда)
   ▼
_score_or_collect      фетч повного тексту статті (trafilatura) + og-медіа
   │                   + embed-відео (YouTube/Vimeo, лише лінк)
   │
   ├─ ranking.enabled=false:  postmaker.make() — повний пост одразу
   │                          → _decide_and_store (editorial + поріг min_post_score)
   │
   └─ ranking.enabled=true:   postmaker.classify() — дешева класифікація,
                              status='candidate' (noise/promo → filtered;
                              breaking + ≥2 джерел → негайний повний шлях)
                                 │
                              NewsRanker (кожні cycle_minutes): один виклик gpt-4o
                              обирає top-N → повний пост → _decide_and_store
                              (editorial = ПІСЛЯ-перевірка з правом вето);
                              програвші за max_age_hours → wrap/digest
   ▼
_decide_and_store      editorial_policy.decide_publish_mode: квоти по темах,
   │                   сатурація, origin-баланс 50/50, затримка non-breaking
   │                   → pending_post | pending_wrap | digest_only | filtered
   ▼
_publish_due           retry error-items (10/20/40 хв, потім failed + алерт)
   │                   → дозрілі pending_post:
   │                     [moderation.enabled] → прев'ю в admin-чат (✅/✏️/❌)
   │                     інакше → канал (відео → фото → текст, ChannelPublisher)
   │                   → wraps: по категоріях або [clustering.enabled] по сюжетах
   ▼
канал + post_metrics (engagement) + funnel_daily + щоденний digest/звіт
```

Статус-машина news_items: `new → candidate/pending_post/pending_wrap/digest_only/
pending_review → posted/wrapped/rejected/filtered/error → failed`.

## Запуск

```bash
pip install -r requirements.txt
# .env: TELEGRAM_TOKEN, OPENAI_API_KEY (опційно TELEGRAM_ADMIN_CHAT_ID)
python main.py
```

Тести: `make test` або `scripts\test.ps1` (Windows), або просто `python -m pytest`
(dev-залежності: `pip install -r requirements-dev.txt`).

Перевірка здоров'я фідів: `python scripts/validate_sources.py` (`make validate-sources`).

## Секції config.yaml

| Секція | Призначення |
|---|---|
| `telegram` | chat_id каналу, admin_chat_id адмінки (токен — у .env) |
| `openai` | модель embeddings |
| `categories` | рубрики: slug/title/hashtag |
| `llm` | моделі і промпти post/wrap/digest (overrides — `/prompt_set`) |
| `sources` | RSS-фіди з `origin: ua\|world` для балансу 50/50 |
| `filters` | глобальні deny-регекспи по title/url/summary |
| `posting` | ліміти за цикл, freshness-вікно, cluster_wait, wrap_rules по категоріях |
| `images`, `media`, `articles` | og-фетч, ліміти розмірів медіа, повний текст статті |
| `db` | шлях до SQLite і retention |
| `network`, `app`, `monitor` | таймаути, лог-рівень, частота циклів/полінгу |
| `embeddings`, `text_processing` | пороги semantic/lexical дедуплікації |
| `editorial` | min_post_score, квоти по темах, сатурація, origin-баланс, затримки |
| `analytics` | щоденний звіт і команди адмінки |
| `moderation` | **Фаза 1**: human-in-the-loop перед публікацією |
| `engagement` | **Фаза 2**: реакції/знімки post_metrics, опційний MTProto |
| `ranking` | **Фаза 3**: порівняльний відбір замість порога score |
| `clustering` | **Фаза 4**: кластери подій по embeddings, wraps по сюжетах |

## Фіче-флаги (як вмикати фази)

Усі нові секції мають дефолти — стара конфігурація запускається без правок.

| Флаг | Дефолт | Що вмикає |
|---|---|---|
| `moderation.enabled` | false (у репо-конфігу true) | прев'ю постів в admin-чат з кнопками ✅/✏️/❌; `/moderation on\|off` без рестарту; `on_timeout: skip\|publish` |
| `engagement.enabled` | true | збір реакцій (бот має бути адміном каналу) + знімки post_metrics; блок у щоденному звіті |
| `engagement.mtproto_enabled` | false | views/forwards через Telethon (`pip install telethon` + env `TELEGRAM_API_ID/TELEGRAM_API_HASH/TELEGRAM_MTPROTO_SESSION`); строго опційно |
| `ranking.enabled` | false | пул кандидатів + порівняльний відбір gpt-4o; старий шлях лишається при false |
| `clustering.enabled` | false | ідентичність подій по cluster_id; wraps «розвиток сюжету» |

## Команди адмінки

`/help`, `/analytics`, `/analytics_chart`, `/export_news_items`, `/export_wrap_posts`,
`/moderation on|off`, `/funnel` (funnel-метрики за сьогодні/вчора),
`/prompt_list`, `/prompt_get|set|reset <key>` — ключі включають `post_prompt`,
`classify_prompt`, `ranking_prompt`, `cluster_wrap_prompt`, wrap/digest-промпти.
Overrides з БД мають пріоритет над config.yaml і дефолтами в коді.

## Режим модерації (Фаза 1): ручний сценарій

1. `moderation.enabled: true`, `telegram.admin_chat_id` — твій чат із ботом.
2. `python main.py`; у логах чекай `Item ... sent to moderation instead of publishing`.
3. В admin-чат прийде прев'ю `🛂 Модерація · score=… · ua|world` з кнопками.
4. ✏️ — нове прев'ю з переписаним текстом (той самий article_text, таймер скинуто).
5. ✅ — миттєва публікація в канал; ❌ — status='rejected', у канал нічого.
6. Мовчання понад `timeout_minutes` → політика `on_timeout` (skip → rejected,
   publish → автопублікація), лог `Moderation timeouts handled`.
7. Рішення — у таблиці `moderation_log` (датасет для калібрування порогів).

## Engagement (Фаза 2)

Реакції — через `message_reaction` updates наявного getUpdates-полінгу (без
webhooks). Раз на `poll_hours` — append-only знімок у `post_metrics`. Views і
forwards Bot API боту не віддає: або MTProto-шлях (див. флаг вище), або тиха
деградація з warning раз на добу. Щоденний звіт отримує топ-5/анти-топ-5 і
розрізи по category/topic_key/origin.

## Ранжування (Фаза 3)

Кожні `cycle_minutes` ранкер передає в gpt-4o компактний пул (id, title,
fact_summary, source, origin, вік, source_count) + поточний origin_share і
останні 10 topic_key — і отримує top-`max_picks` з обґрунтуванням. Переможці
проходять editorial як після-перевірку (вето). Breaking з ≥2 джерелами минає
чергу. Програвші за `max_age_hours` → wrap/digest. Зламаний JSON → 1 retry →
fallback на старий поріг для циклу. Лог циклів — таблиця `ranking_log`.

## Кластеризація (Фаза 4)

Item з embedding приєднується до найближчого кластера (cosine ≥ `threshold` до
центроїда; центроїд — інкрементальне середнє) або відкриває новий. Канонічний
представник — найраніший item з найповнішим article_text. `source_count` і
new-fact gate рахуються по кластеру; event_key лишається довідковим. Wrap —
кластер ≥ `wrap.min_items` items від ≥ `wrap.min_sources` джерел: пост
«розвиток сюжету» з хронології (titles + facts за часом).

## Інженерна гігієна (Фаза 5)

- Error-items ретраяться до 3 разів (паузи 10/20/40 хв), далі `failed` + алерт.
- Funnel-метрики: рядок `Funnel: fetched=… new=… …` наприкінці кожного run_once,
  таблиця `funnel_daily`, команда `/funnel`.
- run_once декомпозовано: `_ingest` → `_match_and_dedup` → `_score_or_collect`
  → `_decide_and_store` → `_publish_due`; медіа-логіка — у `ChannelPublisher`.
- Embed-відео (YouTube/Vimeo) не завантажується — у пост додається рядок
  `▶️ Відео: <лінк>` (колонка `embed_video_url`).
- Мертвий код видалено: `app/translate/` (не використовувався), `test.py`.

## Розробка

- Нічого не публікуй у реальний канал під час розробки: ручні перевірки — через
  `admin_chat_id` (режим модерації перехоплює пости до каналу).
- Зовнішні виклики у тестах — лише фейки (`tests/conftest.py`: FakeTelegramClient,
  DummyHttp, FakePostmaker, FakeRankingLLM, FakeWrapmaker).
- Міграції БД: тільки `_ensure_extra_columns` (ідемпотентні ALTER/CREATE);
  `DESIRED_COLS`/`SCHEMA_V5` не чіпати. Смоук — `tests/test_migrations.py`.
