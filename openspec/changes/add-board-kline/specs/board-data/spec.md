## ADDED Requirements

### Requirement: Board List Query

The system SHALL provide a command to query all industry and concept boards.

#### Scenario: List industry boards
- **WHEN** user runs `pichip board list --type industry`
- **THEN** the system displays all industry boards with code, name, change%, turnover rate

#### Scenario: List concept boards
- **WHEN** user runs `pichip board list --type concept`
- **THEN** the system displays all concept boards with code, name, change%, turnover rate

### Requirement: Board Historical K-Line Data Sync

The system SHALL provide a command to sync board historical K-line data to local cache.

#### Scenario: Sync all boards
- **WHEN** user runs `pichip board sync`
- **THEN** the system syncs all board K-line data from akshare API to local SQLite database

#### Scenario: Sync with date range
- **WHEN** user runs `pichip board sync --start-date 20240101 --end-date 20241231`
- **THEN** the system syncs board data within the specified date range

#### Scenario: Sync specific board type
- **WHEN** user runs `pichip board sync --type industry`
- **THEN** the system only syncs industry board data

### Requirement: Board Data Query

The system SHALL provide a command to query board K-line data and indicators.

#### Scenario: Show board K-line
- **WHEN** user runs `pichip board show 半导体`
- **THEN** the system displays K-line data and basic indicators (MA, MACD) for the specified board

#### Scenario: Show with days limit
- **WHEN** user runs `pichip board show 半导体 --days 30`
- **THEN** the system displays the last 30 days of K-line data

### Requirement: Board Data Cache

The system SHALL cache board data in SQLite database for offline access and faster queries.

#### Scenario: Cache board info
- **WHEN** syncing board data
- **THEN** the system saves board info (code, name, type) to `board_info` table

#### Scenario: Cache board K-line
- **WHEN** syncing board data
- **THEN** the system saves K-line data to `board_daily` table

#### Scenario: Incremental sync
- **WHEN** user syncs data that already exists in cache
- **THEN** the system skips existing data and only syncs new data
