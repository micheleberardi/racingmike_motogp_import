# MotoGP API Documentation 

This README provides detailed instructions on how to use various MotoGP APIs to retrieve results, seasons, categories, events, and session details.

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

### Usage

To use these APIs, replace `<season_id>`, `<event_id>`, and `<category_id>` with the appropriate IDs retrieved from the previous API calls.

### Example

To get a list of categories for the 2023 MotoGP season:

1. First, call the 'List Seasons' endpoint to get the season ID for 2023.
2. Use this ID in the 'List Categories' endpoint to fetch the categories for that season.

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details.
