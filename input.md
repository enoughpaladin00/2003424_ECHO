# System Description:
 
**E.C.H.O. (Earthquake & Conflict Hazard Observer)** is a distributed monitoring platform that allows real-time interaction between a network of simulated seismic sensors and a centralized command dashboard, with the main purpose of detecting and classifying geopolitical threats.

The system will automatically process raw, high-frequency data streams to categorize anomalies into natural occurrences, conventional military conflicts, or nuclear-like events. Simultaneously, the System Administrators will be able to manage a fault-tolerant backend infrastructure to ensure continuous data routing, and the Intelligence Analysts to monitor the live feeds, filter historical data, and export strategic reports directly from the platform.

##  Defined Roles
To write effective user stories, we can identify three primary roles based on the geopolitical scenario and the technical constraints of the project:

1. **Intelligence Analyst (End-User):** The personnel stationed at the neutral command center who relies on the dashboard to monitor threats and analyze seismic events.

2. **System Administrator (Operator):** The technical user responsible for deploying the system via Docker, ensuring the gateway routes traffic properly, and maintaining fault tolerance across distributed replicas.

3. **Seismic Simulator (System Actor):** The external containerized service that emits the real-time WebSocket data streams and generates the simulated SSE shutdown commands.

# User Stories:

1) As a System Administrator, I want a unified docker-compose.yml file to spin up all microservices and the simulator with a single command so that deployment is standardized.
2) As a System Administrator, I want to implement a CI/CD pipeline (e.g., GitHub Actions) that runs automated unit tests on every pull request so that broken code is not merged into the main branch.
3) As an Intelligence Analyst, I want the system to expose a single API Gateway so that my frontend requests are seamlessly and securely routed to the correct backend services.
4) As a System Administrator, I want the API Gateway to implement rate limiting so that the system is protected against accidental denial-of-service from misconfigured clients.
5) As a System Administrator, I want the custom broker to connect to the simulator's WebSocket endpoints so that the system continuously ingests real-time raw seismic measurements.
6) As a System Administrator, I want the broker to fan-out (redistribute) incoming measurements to multiple processing replicas so that the processing load is balanced.
7) As a System Administrator, I want the broker to automatically attempt reconnection with exponential backoff if the simulator connection drops, ensuring data ingestion resumes seamlessly.
8) As a System Administrator, I want the broker to implement a dead-letter queue for malformed measurements so that invalid data does not crash the processing replicas.
9) As the System, I need to maintain an in-memory sliding window of recent time-domain measurements for each sensor to prepare the data for frequency analysis.
10) As the System, I need to apply a Discrete Fourier Transform (DFT) or an equivalent FFT method on the sliding window to extract dominant frequency components.
11) As an Intelligence Analyst, I want the system to automatically classify an event as an "Earthquake" (0.5 to 3.0 Hz) so that natural occurrences are categorized correctly.
12) As an Intelligence Analyst, I want the system to automatically classify an event as a "Conventional explosion" (3.0 to 8.0 Hz) so I can identify potential military conflicts.
13) As an Intelligence Analyst, I want the system to classify an event as a "Nuclear-like event" (>= 8.0 Hz) so that I can immediately identify catastrophic strategic threats.
14) As an Intelligence Analyst, I want the system to calculate and attach a severity score to each event based on the amplitude of the signal so that I can prioritize responses to the strongest events.
15) As a System Administrator, I want the processing replicas to listen to the simulator's control stream via SSE and self-terminate upon receiving a shutdown command so that node failure is accurately simulated.
16) As a System Administrator, I want the Gateway/Broker to actively health-check processing replicas and automatically exclude failed ones from the routing pool so that operations are not interrupted.
17) As a System Administrator, I want new processing replicas to automatically register themselves with the Gateway/Broker upon startup so that the system can dynamically scale up.
18) As a System Administrator, I want to implement a circuit breaker pattern between the microservices so that a cascading failure is prevented if the database or a downstream service becomes unresponsive.
19) As a System Administrator, I want detected events to be stored in a centralized relational or NoSQL database (e.g., PostgreSQL, MongoDB) so that event data is persisted securely.
20) As a System Administrator, I want the database insertion logic to be idempotent so that multiple replicas analyzing the exact same event do not create duplicate database entries.
21) As a System Administrator, I want the database tables/collections to be properly indexed by timestamp and location so that historical queries load quickly on the dashboard.
22) As a System Administrator, I want an automated data-retention script to run daily and archive events older than 30 days so that the active database remains performant.
23) As an Intelligence Analyst, I want to access a real-time web dashboard that pushes new classified events automatically (via WebSocket or SSE) so I do not have to manually refresh the page.
24) As an Intelligence Analyst, I want the dashboard to display critical visual and audio alerts specifically for "Nuclear-like events" so that they command immediate attention.
25) As an Intelligence Analyst, I want to see an interactive map component on the dashboard displaying the geographical origins of the simulated sensors.
26) As an Intelligence Analyst, I want to filter the history of past seismic events by sensor ID, event type, and date range so that I can perform targeted post-incident analysis.
27) As an Intelligence Analyst, I want the dashboard UI to be responsive and accessible so that I can monitor the system from tablets or mobile devices in the command center.
28) As an Intelligence Analyst, I want to filter the dashboard's event feed by Sensor ID, Event Type, and Location, so that I can cut through the noise and focus my analysis on specific high-risk regions.
29) As a System Administrator, I want the dashboard to require user authentication (login/password) so that unauthorized personnel cannot view sensitive strategic data.
30) As a System Administrator, I want an audit log to record whenever an Analyst logs in or exports data so that system access is tracked.
31) As an Intelligence Analyst, I want a button on the dashboard to export filtered historical data as a CSV or PDF report so that I can share findings with external commanders.

## Event Schema

This table defines the structured data payload for any seismic event processed and classified by the E.C.H.O. system. All microservices (Message Broker, Processing Replicas, Database, and Command Dashboard) must adhere to this structure.

| Field Name | Data Type | Description |
| --- |:---:| :---:|
|`eventId` | UUID (String) | A universally unique identifier for the event. Used primarily by the database to ensure idempotency and prevent duplicate records from multiple processing replicas. |
| `sensorId` | String | The unique identifier of the simulated sensor that detected the initial anomaly. |
| `timestamp` | DateTime (ISO 8601) | The exact UTC timestamp of when the anomaly occurred. Used for historical indexing and time-series filtering on the dashboard. |
| `location` | Object | The geographical coordinates of the sensor origin:`latitude` (Float), `longitude` (Float).|
| `dominantFrequencyHz` | Float | The primary frequency extracted from the raw data stream using DFT/FFT analysis. |
| `eventType` | Enum (String) | The classification of the event based on the dominant frequency. Allowed values: `"Earthquakes"`, `"Conventional explosion"`, `"Nuclear-like event"` |
| `severityScore` | Float | A calculated priority score derived from the signal's amplitude, used for alert prioritization. |

### JSON Payload
```JSON
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "ClassifiedSeismicEvent",
  "type": "object",
  "properties": {
    "eventId": {
      "type": "string",
      "description": "Unique UUID for the classified event to prevent database duplication."
    },
    "sensorId": {
      "type": "string",
      "description": "Identifier of the simulated sensor."
    },
    "timestamp": {
      "type": "string",
      "format": "date-time",
      "description": "ISO 8601 timestamp of when the anomaly occurred."
    },
    "location": {
      "type": "object",
      "properties": {
        "latitude": { "type": "number" },
        "longitude": { "type": "number" }
      },
      "required": ["latitude", "longitude"]
    },
    "dominantFrequencyHz": {
      "type": "number",
      "description": "The primary frequency extracted via DFT/FFT analysis."
    },
    "eventType": {
      "type": "string",
      "enum": ["Earthquake", "Conventional explosion", "Nuclear-like event"]
    },
    "severityScore": {
      "type": "number",
      "description": "Calculated priority score based on signal amplitude."
    }
  },
  "required": ["eventId", "sensorId", "timestamp", "location", "dominantFrequencyHz", "eventType", "severityScore"]
}
```

## Rule Model
The Rule Model defines the central logical conditions and business rules that govern how the E.C.H.O. system interprets data, triggers actions and manages infrastractures. Based on the defined user stories, the system's logic is divided into three primary domains:

### A. Classification Rules (Signal Processing Engine)
These rules are applied by the processing replicas after extracting the dominant frequency using DFT/FFT methods on the sliding window:

- **IF** the dominant frequency is between 0.5 and 3.0 Hz, **THEN** the system automatically classifies the event as an "Earthquake".

- **IF** the dominant frequency is between 3.0 and 8.0 Hz, **THEN** the system automatically classifies the event as an "Conventional explosion".

- **IF** the dominant frequency is greater than 8.0., **THEN** the system automatically classifies the event as an "Nuclear-like event".

### B: Alerting & Data Routing Rules
These rules govern how the system handles classified data and interacts with the dashboard:

- **IF** an event is classified as a "Nuclear-like event", **THEN** the dashboard must display critical visual and audio alerts.

- **IF** multiple replicas analyze the exact same event, **THEN** the database insertion logic must be idempotent to prevent duplicate entries.

- **IF** a raw measurement is malformed, **THEN** the broker must route it to a dead-letter queue so it does not crash the processing replicas.

### C. Infrastracture & DevOps Rules
These rules dictate the fault-tolerant behavior of the microservices infrastructure:

- **IF** a processing replica receives a shutdown command from the simulator's SSE control stream, **THEN** it must self-terminate to accurately simulate node failure.

- **IF** a processing replica fails an active health check, **THEN** the Gateway/Broker must automatically exclude it from the routing pool.

- **IF** a new processing replica starts up, **THEN** it must automatically register itself with the Gateway/Broker to allow dynamic scaling.

- **IF** the simulator connection drops, **THEN** the broker must automatically attempt reconnection using exponential backoff.

- **IF** a downstream service or database becomes unresponsive, **THEN** a circuit breaker pattern must trigger to prevent a cascading failure.

- **IF** an event in the database becomes older than 30 days, **THEN** an automated data-retention script must archive it daily.