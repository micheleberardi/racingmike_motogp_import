# MotoGP Import Scripts

Script Python per importare dati MotoGP in MySQL (seasons, events, categories, sessions, results, standings, live timing).

## Requisiti

- Python 3.10+
- MySQL raggiungibile
- Dipendenze Python installate (`pymysql`, `requests`, `python-dotenv`, `urllib3`)

## Configurazione

Crea un file `.env` nella root progetto con:

```env
DB_HOST=localhost
DB_PORT=3306
DB_USER=...
DB_PASSWD=...
# oppure DB_PASSWORD=...
DB_NAME=...
DB_CHARSET=utf8mb4
TARGET_YEAR=2026
```

`TARGET_YEAR` e' opzionale. Se non presente, gli script usano l'anno corrente UTC.

## Esecuzione

Ogni script supporta `--year`:

```bash
python3 seasons.py --year 2026
python3 events.py --year 2026
python3 categories_general.py --year 2026
python3 categories_by_event.py --year 2026
python3 categories_by_season.py --year 2026
python3 sessions.py --year 2026
python3 results.py --year 2026
python3 standingriders.py --year 2026
python3 teams.py --year 2026
python3 sessions_test.py --year 2026
```

Script senza filtro anno:

```bash
python3 calendar2024.py
python3 liveresult.py
```

## Migliorie introdotte

- anno hardcoded rimosso dagli script
- timeout/retry HTTP centralizzati
- gestione errori piu' robusta
- transazioni DB coerenti con commit finale
- parsing JSON difensivo su campi opzionali
- logging uniforme

## Avvertenza

Gli script scrivono direttamente sulle tabelle di produzione: esegui sempre prima su ambiente staging quando cambi schema.
https://api.motogp.pulselive.com/motogp/v1/results/sessions?eventUuid=f3fd8ba7-2966-46bd-8687-b92047f5e733&categoryUuid=e8c110ad-64aa-4e8e-8a86-f2f152f6a942