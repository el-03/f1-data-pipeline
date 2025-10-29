-- Drop the schema if it exists
DROP SCHEMA IF EXISTS formula_one CASCADE;

-- Recreate schema
CREATE SCHEMA IF NOT EXISTS formula_one;

-- Enum type for session types
CREATE TYPE formula_one.char_session_type AS ENUM (
  'R',
  'Q1',
  'Q2',
  'Q3',
  'QA',
  'QO',
  'QB',
  'FP1',
  'FP2',
  'FP3',
  'PQ',
  'SR',
  'SQ1',
  'SQ2',
  'SQ3'
);

-- Driver table
CREATE TABLE formula_one.driver (
  id SERIAL PRIMARY KEY,
  forename VARCHAR NOT NULL,
  surname VARCHAR NOT NULL,
  reference VARCHAR NOT NULL UNIQUE,
  abbreviation VARCHAR(5),
  nationality VARCHAR,
  country_code CHAR(3),
  permanent_car_number SMALLINT,
  date_of_birth DATE
);

-- Team table
CREATE TABLE formula_one.team (
  id SERIAL PRIMARY KEY,
  name VARCHAR NOT NULL,
  nationality VARCHAR,
  reference VARCHAR NOT NULL UNIQUE,
  country_code CHAR(3)
);

-- Season table
CREATE TABLE formula_one.season (
  id SERIAL PRIMARY KEY,
  year INTEGER NOT NULL UNIQUE
);

-- Circuit table
CREATE TABLE formula_one.circuit (
  id SERIAL PRIMARY KEY,
  name VARCHAR NOT NULL,
  reference VARCHAR NOT NULL UNIQUE,
  country VARCHAR,
  country_code CHAR(3),
  locality VARCHAR,
  latitude FLOAT,
  longitude FLOAT,
  altitude FLOAT
);

-- Round table
CREATE TABLE formula_one.round (
  id SERIAL PRIMARY KEY,
  season_id INTEGER REFERENCES formula_one.season(id) ON DELETE SET NULL,
  circuit_id INTEGER REFERENCES formula_one.circuit(id) ON DELETE SET NULL,
  name VARCHAR,
  date DATE,
  number INTEGER,
  race_number INTEGER
);

-- Session table
CREATE TABLE formula_one.session (
  id SERIAL PRIMARY KEY,
  round_id INTEGER REFERENCES formula_one.round(id) ON DELETE SET NULL,
  number INTEGER,
  type formula_one.char_session_type NOT NULL,
  scheduled_laps INTEGER,
  timestamp TIMESTAMP,
  timezone VARCHAR,
  is_cancelled BOOLEAN NOT NULL DEFAULT FALSE
);

-- Team-driver relationship table
CREATE TABLE formula_one.team_driver (
  id SERIAL PRIMARY KEY,
  team_id INTEGER REFERENCES formula_one.team(id) ON DELETE SET NULL,
  driver_id INTEGER REFERENCES formula_one.driver(id) ON DELETE SET NULL,
  season_id INTEGER REFERENCES formula_one.season(id) ON DELETE SET NULL
);

-- Driver championship table
CREATE TABLE formula_one.driver_championship (
  id SERIAL PRIMARY KEY,
  season_id INTEGER REFERENCES formula_one.season(id) ON DELETE SET NULL,
  round_id INTEGER REFERENCES formula_one.round(id) ON DELETE SET NULL,
  session_id INTEGER REFERENCES formula_one.session(id) ON DELETE SET NULL,
  driver_id INTEGER REFERENCES formula_one.driver(id) ON DELETE SET NULL,
  round_number INTEGER NOT NULL,
  session_number INTEGER NOT NULL,
  year INTEGER NOT NULL,
  position SMALLINT,
  points FLOAT NOT NULL DEFAULT 0,
  win_count INTEGER NOT NULL DEFAULT 0,
  CONSTRAINT unique_driver_championship UNIQUE (season_id, round_id, driver_id);
);

-- Team championship table
CREATE TABLE formula_one.team_championship (
  id SERIAL PRIMARY KEY,
  season_id INTEGER REFERENCES formula_one.season(id) ON DELETE SET NULL,
  round_id INTEGER REFERENCES formula_one.round(id) ON DELETE SET NULL,
  session_id INTEGER REFERENCES formula_one.session(id) ON DELETE SET NULL,
  team_id INTEGER REFERENCES formula_one.team(id) ON DELETE SET NULL,
  round_number INTEGER NOT NULL,
  session_number INTEGER NOT NULL,
  year INTEGER NOT NULL,
  position INTEGER,
  points FLOAT NOT NULL DEFAULT 0,
  win_count INTEGER NOT NULL DEFAULT 0,
  CONSTRAINT unique_driver_championship UNIQUE (season_id, round_id, team_id)
);

-- ============================================
-- RACE RESULTS
-- ============================================
CREATE TABLE formula_one.race_result (
  id SERIAL PRIMARY KEY,
  season_id INTEGER NOT NULL,
  round_id INTEGER NOT NULL,
  session_id INTEGER,  -- FK to session table
  driver_id INTEGER NOT NULL,
  team_id INTEGER NOT NULL,

  -- Results
  position INTEGER,  -- Final position (NULL if DNF)
  position_text VARCHAR(10),  -- '1', '2', 'R' (retired), 'D' (disqualified)
  points DECIMAL(5,2) NOT NULL DEFAULT 0,

  -- Performance
  grid_position INTEGER,  -- Starting grid position
  laps_completed INTEGER NOT NULL DEFAULT 0,
  status VARCHAR(50),  -- 'Finished', '+1 Lap', 'Accident', 'Engine', etc.

  -- Times
  race_time_milliseconds BIGINT,  -- Total race time in ms (winner only)
  fastest_lap_time VARCHAR(20),  -- Best lap time (e.g., '1:23.456')
  fastest_lap_milliseconds BIGINT,  -- Fastest lap time in ms
  fastest_lap_number INTEGER,  -- Which lap was fastest
  fastest_lap_rank INTEGER,  -- Rank for fastest lap (1 = fastest overall)

  CONSTRAINT fk_race_season FOREIGN KEY (season_id) REFERENCES formula_one.season(id),
  CONSTRAINT fk_race_round FOREIGN KEY (round_id) REFERENCES formula_one.round(id),
  CONSTRAINT fk_race_driver FOREIGN KEY (driver_id) REFERENCES formula_one.driver(id),
  CONSTRAINT fk_race_team FOREIGN KEY (team_id) REFERENCES formula_one.team(id),
  CONSTRAINT fk_race_session FOREIGN KEY (session_id) REFERENCES formula_one.session(id),

  -- Unique constraint: one result per driver per race
  CONSTRAINT unique_race_result UNIQUE (season_id, round_id, driver_id)
);

CREATE INDEX idx_race_result_season_round ON formula_one.race_result(season_id, round_id);
CREATE INDEX idx_race_result_driver ON formula_one.race_result(driver_id);
CREATE INDEX idx_race_result_team ON formula_one.race_result(team_id);

-- ============================================
-- QUALIFYING RESULTS
-- ============================================
CREATE TABLE formula_one.qualifying_result (
  id SERIAL PRIMARY KEY,
  season_id INTEGER NOT NULL,
  round_id INTEGER NOT NULL,
  last_session_id INTEGER,
  driver_id INTEGER NOT NULL,
  team_id INTEGER NOT NULL,

  -- Final qualifying position
  position INTEGER NOT NULL,

  -- Session times (best lap in each session)
  q1_time VARCHAR(20),  -- Q1 best time (e.g., '1:23.456')
  q1_time_milliseconds BIGINT,

  q2_time VARCHAR(20),  -- Q2 best time (NULL if didn't make it to Q2)
  q2_time_milliseconds BIGINT,

  q3_time VARCHAR(20),  -- Q3 best time (NULL if didn't make it to Q3)
  q3_time_milliseconds BIGINT,

  CONSTRAINT fk_quali_season FOREIGN KEY (season_id) REFERENCES formula_one.season(id),
  CONSTRAINT fk_quali_round FOREIGN KEY (round_id) REFERENCES formula_one.round(id),
  CONSTRAINT fk_quali_driver FOREIGN KEY (driver_id) REFERENCES formula_one.driver(id),
  CONSTRAINT fk_quali_team FOREIGN KEY (team_id) REFERENCES formula_one.team(id),
  CONSTRAINT fk_quali_session FOREIGN KEY (last_session_id) REFERENCES formula_one.session(id),

  -- Unique constraint: one result per driver per qualifying session
  CONSTRAINT unique_quali_result UNIQUE (season_id, round_id, driver_id)
);

CREATE INDEX idx_quali_result_season_round ON formula_one.qualifying_result(season_id, round_id);
CREATE INDEX idx_quali_result_driver ON formula_one.qualifying_result(driver_id);
CREATE INDEX idx_quali_result_position ON formula_one.qualifying_result(position);

-- ============================================
-- SPRINT RACE RESULTS
-- ============================================
CREATE TABLE formula_one.sprint_result (
  id SERIAL PRIMARY KEY,
  season_id INTEGER NOT NULL,
  round_id INTEGER NOT NULL,
  session_id INTEGER,
  driver_id INTEGER NOT NULL,
  team_id INTEGER NOT NULL,

  -- Results
  position INTEGER,  -- Final position (NULL if DNF)
  position_text VARCHAR(10),
  position_order INTEGER NOT NULL,
  points DECIMAL(5,2) NOT NULL DEFAULT 0,

  -- Performance
  grid_position INTEGER,  -- Starting position for sprint
  laps_completed INTEGER NOT NULL DEFAULT 0,
  status VARCHAR(50),

  -- Times
  sprint_time_milliseconds BIGINT,  -- Total sprint time

  CONSTRAINT fk_sprint_season FOREIGN KEY (season_id) REFERENCES formula_one.season(id),
  CONSTRAINT fk_sprint_round FOREIGN KEY (round_id) REFERENCES formula_one.round(id),
  CONSTRAINT fk_sprint_driver FOREIGN KEY (driver_id) REFERENCES formula_one.driver(id),
  CONSTRAINT fk_sprint_team FOREIGN KEY (team_id) REFERENCES formula_one.team(id),
  CONSTRAINT fk_sprint_session FOREIGN KEY (session_id) REFERENCES formula_one.session(id),

  -- Unique constraint: one result per driver per sprint
  CONSTRAINT unique_sprint_result UNIQUE (season_id, round_id, driver_id)
);

CREATE INDEX idx_sprint_result_season_round ON formula_one.sprint_result(season_id, round_id);
CREATE INDEX idx_sprint_result_driver ON formula_one.sprint_result(driver_id);
CREATE INDEX idx_quali_result_round_pos ON formula_one.qualifying_result(round_id, position);


-- Create indexes for better query performance
CREATE INDEX idx_round_season ON formula_one.round(season_id);
CREATE INDEX idx_session_round ON formula_one.session(round_id);
CREATE INDEX idx_driver_champ_season ON formula_one.driver_championship(season_id, year);
CREATE INDEX idx_team_champ_season ON formula_one.team_championship(season_id, year);