# MotoGP API Documentation     
 

This README provides detailed instructions on how to use various MotoGP APIs to retrieve results, seasons, categories, events, and session details.

This API are from MOTOGP OFFICIAL WEBSITE

on my github you can find the code to import the data from the API to the database.

## Getting Started

These instructions will guide you through the process of fetching data from the MotoGP APIs.

### Prerequisites

Before you start, ensure you have a stable internet connection and an API client like Postman or a similar tool to make requests to the provided URLs.

### API Endpoints

#### 1. List Seasons

- **Endpoint**: `https://api.motogp.pulselive.com/motogp/v1/results/seasons`
- **Description**: Retrieves a list of MotoGP seasons.
- **Response Example**:
    ```json
    {
      "id": "db8dc197-c7b2-4c1b-b3a4-6dc534c023ef",
      "name": null,
      "year": 2023,
      "current": true
    }
    ```

#### 2. List Categories for a Season

- **Endpoint**: `https://api.motogp.pulselive.com/motogp/v1/results/categories?seasonUuid=<season_id>`
- **Description**: Fetches categories for a specific season.
- **Response Example**:
    ```json
    [
      {"id": "e8c110ad-64aa-4e8e-8a86-f2f152f6a942", "name": "MotoGP™", "legacy_id": 3},
      ...
    ]
    ```

#### 3. List Events

- **Endpoint**: `https://api.motogp.pulselive.com/motogp/v1/results/events?seasonUuid=<season_id>&isFinished=true`
- **Description**: Retrieves a list of events for a given season.
- **Response Example**:
    ```json
    [
      {
        "country": {"iso": "PT", "name": "Portugal"},
        "event_files": {...},
        ...
      }
    ]
    ```

#### 4. List Sessions

- **Endpoint**: `https://api.motogp.pulselive.com/motogp/v1/results/sessions?eventUuid=<event_id>&categoryUuid=<category_id>`
- **Description**: Fetches session details for a given event and category.
- **Response Example**:
    ```json
    [
      {
        "date": "2023-03-24T10:45:00+00:00",
        "number": 1,
        "condition": {...},
        ...
      }
    ]
    ```


---

#### 5. Getting Race Session Results

This section explains how to fetch results for a particular MotoGP race session.

### API Endpoint for Race Session Results

- **Endpoint**: `https://api.motogp.pulselive.com/motogp/v1/results/session/<session_id>/classification?test=false`
- **Method**: `GET`
- **Description**: Retrieves the classification or results of a specific race session.
- **Parameters**:
    - `session_id`: The unique identifier for the session.

### Example

To retrieve results for a specific race session, use the session ID obtained from the 'List Sessions' API. For instance, to get results for the session with the ID `cb7655d9-387b-4247-9bbe-a067bbe484ff`, the request URL will be:

```
https://api.motogp.pulselive.com/motogp/v1/results/session/cb7655d9-387b-4247-9bbe-a067bbe484ff/classification?test=false
```

### Sample Response

A successful request will return a JSON response containing the classification of riders for the session. Here is a snippet of what the response might look like:

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
        // more rider details...
      },
      "team": {
        "name": "Gresini Racing MotoGP",
        // more team details...
      },
      "constructor": {
        "name": "Ducati",
        // more constructor details...
      },
      "best_lap": {"number": 12, "time": "01:38.7820"},
      "total_laps": 15,
      "top_speed": 339.6,
      "gap": {"first": "0.000", "prev": "0.000"},
      "status": "INSTND"
    },
    // more riders...
  ]
}
```



#### 6. Retrieving MotoGP Season Standings

This section details how to obtain the standings for a specific MotoGP season and category.

### API Endpoint for Standings

- **Endpoint**: `https://api.motogp.pulselive.com/motogp/v1/results/standings?seasonUuid=<season_id>&categoryUuid=<category_id>`
- **Method**: `GET`
- **Description**: Fetches the standings for a particular MotoGP season and category.
- **Parameters**:
    - `seasonUuid`: The unique identifier for the season.
    - `categoryUuid`: The unique identifier for the category.

### Example

To get the standings for the MotoGP category in the 2023 season, use the following request URL:

```
https://api.motogp.pulselive.com/motogp/v1/results/standings?seasonUuid=db8dc197-c7b2-4c1b-b3a4-6dc534c023ef&categoryUuid=e8c110ad-64aa-4e8e-8a86-f2f152f6a942
```

### Sample Response

The API response provides details about the riders' standings, including their position, points, team, and more. Here's a snippet of the response:

```json
{
  "classification": [
    {
      "position": 1,
      "rider": {"full_name": "Francesco Bagnaia", "country": {"iso": "IT", "name": "Italy"}, "number": 1},
      "team": {"name": "Ducati Lenovo Team"},
      "constructor": {"name": "Ducati"},
      "session": "RAC",
      "points": 412
    },
    // More riders...
  ],
  "file": "https://resources.motogp.com/files/results/2023/MAL/MotoGP/RAC/worldstanding.pdf"
}
```



## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details.

## Data Attribution and Disclaimer
#### Data Source
The data utilized in this project is sourced from the official MotoGP APIs. This includes race results, standings, and other related information.

## Ownership and Rights
All data provided through these APIs is the property of MotoGP or its respective owners. The use of this data within this project is for informational and non-commercial purposes.

## Disclaimer
This project is not affiliated with, endorsed by, or in any way officially connected with MotoGP or any of its subsidiaries or affiliates. The official MotoGP website can be found at https://www.motogp.com.

## Contact and Issues
If there are any concerns or questions regarding the use of MotoGP data in this project, or if it is believed that the data usage infringes on any rights or terms of use, please reach out to mike@hackmike.com for immediate action.