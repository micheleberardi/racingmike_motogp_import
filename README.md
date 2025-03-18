Here’s a cleaner, more polished version of the README:

---

# MotoGP API Documentation

This README provides detailed instructions on how to use the MotoGP API to retrieve data on results, seasons, categories, events, and session details.

**Note:** The APIs are sourced from the **official MotoGP website**.

On my GitHub, you can find the code to import data from the API into a database.

---

## Getting Started

Follow these instructions to fetch data from the MotoGP APIs.

### Prerequisites

Before you start, make sure you have:

- A stable internet connection  
- An API client like **Postman** or any similar tool for making requests

---

## API Endpoints

### 1. List Seasons
- **Endpoint:**  
  `https://api.motogp.pulselive.com/motogp/v1/results/seasons`

- **Description:**  
  Retrieves a list of available MotoGP seasons.

- **Sample Response:**
```json
{
  "id": "db8dc197-c7b2-4c1b-b3a4-6dc534c023ef",
  "name": null,
  "year": 2023,
  "current": true
}
```

---

### 2. List Categories for a Season
- **Endpoint:**  
  `https://api.motogp.pulselive.com/motogp/v1/results/categories?seasonUuid=<season_id>`

- **Description:**  
  Retrieves a list of categories for a specific season.

- **Sample Response:**
```json
[
  {"id": "e8c110ad-64aa-4e8e-8a86-f2f152f6a942", "name": "MotoGP™", "legacy_id": 3}
]
```

---

### 3. List Events for a Season
- **Endpoint:**  
  `https://api.motogp.pulselive.com/motogp/v1/results/events?seasonUuid=<season_id>&isFinished=true`

- **Description:**  
  Retrieves a list of events for a given season.

- **Sample Response:**
```json
[
  {
    "country": {"iso": "PT", "name": "Portugal"},
    "event_files": {...}
  }
]
```

---

### 4. List Sessions for an Event and Category
- **Endpoint:**  
  `https://api.motogp.pulselive.com/motogp/v1/results/sessions?eventUuid=<event_id>&categoryUuid=<category_id>`

- **Description:**  
  Retrieves session details for a specific event and category.

- **Sample Response:**
```json
[
  {
    "date": "2023-03-24T10:45:00+00:00",
    "number": 1,
    "condition": {...}
  }
]
```

---

### 5. Get Race Session Results
- **Endpoint:**  
  `https://api.motogp.pulselive.com/motogp/v1/results/session/<session_id>/classification?test=false`

- **Method:**  
  `GET`

- **Description:**  
  Retrieves the classification or results of a specific race session.

- **Parameters:**
  - `session_id` – The unique identifier for the session.

- **Example:**  
To get results for a session with the ID `cb7655d9-387b-4247-9bbe-a067bbe484ff`:

```
https://api.motogp.pulselive.com/motogp/v1/results/session/cb7655d9-387b-4247-9bbe-a067bbe484ff/classification?test=false
```

- **Sample Response:**
```json
{
  "classification": [
    {
      "id": "f5315e45-c742-4e94-a1d3-5dbee24b5edb",
      "position": 1,
      "rider": {
        "full_name": "Alex Marquez",
        "country": {"iso": "ES", "name": "Spain"},
        "number": 73
      },
      "team": {
        "name": "Gresini Racing MotoGP"
      },
      "constructor": {
        "name": "Ducati"
      },
      "best_lap": {"number": 12, "time": "01:38.7820"},
      "total_laps": 15,
      "top_speed": 339.6,
      "gap": {"first": "0.000", "prev": "0.000"},
      "status": "INSTND"
    }
  ]
}
```

---

### 6. Get Season Standings
- **Endpoint:**  
  `https://api.motogp.pulselive.com/motogp/v1/results/standings?seasonUuid=<season_id>&categoryUuid=<category_id>`

- **Method:**  
  `GET`

- **Description:**  
  Retrieves the standings for a specific MotoGP season and category.

- **Parameters:**
  - `seasonUuid` – Unique identifier for the season.
  - `categoryUuid` – Unique identifier for the category.

- **Example:**  
To get standings for the 2023 MotoGP season:

```
https://api.motogp.pulselive.com/motogp/v1/results/standings?seasonUuid=db8dc197-c7b2-4c1b-b3a4-6dc534c023ef&categoryUuid=e8c110ad-64aa-4e8e-8a86-f2f152f6a942
```

- **Sample Response:**
```json
{
  "classification": [
    {
      "position": 1,
      "rider": {
        "full_name": "Francesco Bagnaia",
        "country": {"iso": "IT", "name": "Italy"},
        "number": 1
      },
      "team": {"name": "Ducati Lenovo Team"},
      "constructor": {"name": "Ducati"},
      "session": "RAC",
      "points": 412
    }
  ],
  "file": "https://resources.motogp.com/files/results/2023/MAL/MotoGP/RAC/worldstanding.pdf"
}
```

---

### 7. Live Timing Data
- **Endpoint:**  
  `https://api.motogp.pulselive.com/motogp/v1/timing-gateway/livetiming-lite`

- **Description:**  
  Provides real-time race timing data.

---

## License
This project is licensed under the **MIT License** – see the [LICENSE.md](LICENSE.md) file for details.

---

## Data Attribution and Disclaimer

### Data Source
The data used in this project is sourced from the official MotoGP API. This includes race results, standings, and related information.

### Ownership and Rights
All data provided through these APIs is the property of MotoGP or its respective owners. The use of this data is for **informational and non-commercial** purposes only.

### Disclaimer
This project is **not affiliated** with, endorsed by, or officially connected with MotoGP or any of its subsidiaries or affiliates.  
➡️ Official MotoGP website: [https://www.motogp.com](https://www.motogp.com)

---

## Contact and Issues
If you have any questions, concerns, or issues with the use of MotoGP data in this project, contact:

📧 **hello@micheleberardi.com**  

---

