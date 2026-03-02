## ADDED Requirements

### Requirement: Hot Board Data Fetching
The system SHALL provide functions to fetch hot board data from multiple sources.

#### Scenario: Fetch dragon tiger list
- **WHEN** user requests dragon tiger list data
- **THEN** the system returns daily dragon tiger list with stock code, name, net buy amount, and reason

#### Scenario: Fetch gainers list
- **WHEN** user requests gainers list
- **THEN** the system returns top 50 stocks by daily gain percentage

#### Scenario: Fetch volume ratio list
- **WHEN** user requests volume ratio list
- **THEN** the system returns top 50 stocks by volume ratio

#### Scenario: Fetch turnover rate list
- **WHEN** user requests turnover rate list
- **THEN** the system returns top 50 stocks by turnover rate

#### Scenario: Fetch continuous limit-up list
- **WHEN** user requests continuous limit-up list
- **THEN** the system returns stocks with consecutive limit-up days

### Requirement: Multi-Board Resonance Scoring
The system SHALL calculate multi-board resonance score based on how many hot boards a stock appears in.

#### Scenario: Stock appears in 2 boards
- **WHEN** a stock appears in exactly 2 hot boards
- **THEN** the system assigns +20 points for multi-board resonance

#### Scenario: Stock appears in 3 boards
- **WHEN** a stock appears in exactly 3 hot boards
- **THEN** the system assigns +30 points for multi-board resonance

#### Scenario: Stock appears in 4+ boards
- **WHEN** a stock appears in 4 or more hot boards
- **THEN** the system assigns +40 points for multi-board resonance

### Requirement: Capital Quality Scoring
The system SHALL analyze dragon tiger list data to score capital quality.

#### Scenario: Institution net buy >= 30M
- **WHEN** institution net buy amount is >= 30,000,000 CNY
- **THEN** the system assigns +15 points for capital quality

#### Scenario: Famous retail investor net buy >= 20M
- **WHEN** famous retail investor (from predefined list) net buy amount is >= 20,000,000 CNY
- **THEN** the system assigns +10 points for capital quality

#### Scenario: Top buyer seat ratio < 30%
- **WHEN** top buyer seat ratio is < 30%
- **THEN** the system assigns +5 points for capital quality

#### Scenario: Lhasa seats detected
- **WHEN** Lhasa-related seats are detected in buyer list
- **THEN** the system deducts 10 points from capital quality score

### Requirement: Technical Pattern Scoring
The system SHALL score technical patterns based on price and volume data.

#### Scenario: Price above 5-day MA
- **WHEN** current price is above 5-day moving average
- **THEN** the system assigns +5 points for technical pattern

#### Scenario: Breakout from platform
- **WHEN** price breaks out from recent platform or previous high
- **THEN** the system assigns +10 points for technical pattern

#### Scenario: Volume-price coordination
- **WHEN** volume increases with price rise
- **THEN** the system assigns +5 points for technical pattern

#### Scenario: High position stagnation
- **WHEN** price has risen >30% and shows stagnation with high volume
- **THEN** the system deducts 15 points from technical pattern score

### Requirement: Sector Effect Scoring
The system SHALL score sector effect based on sector performance.

#### Scenario: Sector with 3+ limit-ups
- **WHEN** the sector has >= 3 limit-up stocks on the day
- **THEN** the system assigns +10 points for sector effect

#### Scenario: Sector index gain > 3%
- **WHEN** the sector index gains > 3%
- **THEN** the system assigns +5 points for sector effect

### Requirement: Risk Filtering
The system SHALL filter out stocks that meet risk conditions.

#### Scenario: ST stock filter
- **WHEN** a stock is marked as ST or *ST
- **THEN** the system excludes it from results

#### Scenario: Consecutive one-word limit-up filter
- **WHEN** a stock has consecutive one-word (一字) limit-up days
- **THEN** the system excludes it from results

#### Scenario: High turnover rate filter
- **WHEN** a stock's turnover rate exceeds 40%
- **THEN** the system excludes it from results

#### Scenario: Top buyer concentration filter
- **WHEN** top buyer seat ratio exceeds 50%
- **THEN** the system excludes it from results

#### Scenario: Sector decline filter
- **WHEN** the stock's sector declines > 2% on the day
- **THEN** the system excludes it from results

#### Scenario: Low price stock filter
- **WHEN** stock price is below 3 CNY
- **THEN** the system excludes it from results

### Requirement: Grade Classification
The system SHALL classify stocks into grades based on total score.

#### Scenario: Grade A classification
- **WHEN** total score is between 80-100
- **THEN** the system classifies the stock as Grade A

#### Scenario: Grade B classification
- **WHEN** total score is between 60-79
- **THEN** the system classifies the stock as Grade B

#### Scenario: Grade C classification
- **WHEN** total score is between 40-59
- **THEN** the system classifies the stock as Grade C

#### Scenario: Grade D classification
- **WHEN** total score is below 40
- **THEN** the system classifies the stock as Grade D

### Requirement: CLI Command for Dragon Tiger List
The system SHALL provide a CLI command to display dragon tiger list data.

#### Scenario: Display daily dragon tiger list
- **WHEN** user runs `pichip lhb`
- **THEN** the system displays today's dragon tiger list in a table format

#### Scenario: Display dragon tiger list for specific date
- **WHEN** user runs `pichip lhb --date 20260224`
- **THEN** the system displays dragon tiger list for the specified date

#### Scenario: Display dragon tiger detail for specific stock
- **WHEN** user runs `pichip lhb --stock 603887`
- **THEN** the system displays dragon tiger detail for the specified stock

### Requirement: CLI Command for Hot Board Scanner
The system SHALL provide a CLI command to scan and score hot board stocks.

#### Scenario: Display hot board scan results
- **WHEN** user runs `pichip hot`
- **THEN** the system displays scanned stocks with scores and grades

#### Scenario: Filter by minimum score
- **WHEN** user runs `pichip hot --min-score 60`
- **THEN** the system only displays stocks with score >= 60

#### Scenario: Filter by grade
- **WHEN** user runs `pichip hot --grade A`
- **THEN** the system only displays Grade A stocks

#### Scenario: Limit result count
- **WHEN** user runs `pichip hot --top-n 10`
- **THEN** the system displays top 10 stocks by score
