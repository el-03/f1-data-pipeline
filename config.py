"""
config.py
Configuration and loading strategies for F1 data pipeline
"""

import os
from dataclasses import dataclass
from enum import Enum
from typing import List
from datetime import datetime

from dotenv import load_dotenv


class LoadStrategy(Enum):
    """Loading frequency strategies"""
    PRE_SEASON = "pre_season"
    POST_RACE = "post_race"


@dataclass
class TableConfig:
    """Configuration for each table"""
    name: str
    schema: str
    strategy: LoadStrategy
    api_endpoint: str
    dependencies: List[str]
    description: str


# All table configurations
TABLES = {
    "circuit": TableConfig(
        name="circuit",
        schema="formula_one",
        strategy=LoadStrategy.PRE_SEASON,
        api_endpoint="",
        dependencies=[],
        description="F1 circuits/tracks - rarely changes"
    ),

    "season": TableConfig(
        name="season",
        schema="formula_one",
        strategy=LoadStrategy.PRE_SEASON,
        api_endpoint="",
        dependencies=[],
        description="F1 seasons - one row per year"
    ),

    "team": TableConfig(
        name="team",
        schema="formula_one",
        strategy=LoadStrategy.PRE_SEASON,
        api_endpoint="",
        dependencies=["season"],
        description="F1 teams/constructors"
    ),

    "round": TableConfig(
        name="round",
        schema="formula_one",
        strategy=LoadStrategy.PRE_SEASON,
        api_endpoint="",
        dependencies=["season", "circuit"],
        description="Race calendar - rounds/races per season"
    ),

    "session": TableConfig(
        name="session",
        schema="formula_one",
        strategy=LoadStrategy.PRE_SEASON,
        api_endpoint="",
        dependencies=["round"],
        description="Practice, Qualifying, Race sessions"
    ),

    "driver": TableConfig(
        name="driver",
        schema="formula_one",
        strategy=LoadStrategy.PRE_SEASON,
        api_endpoint="/{year}/{round}/constructors/{constructorId}/drivers.json",
        dependencies=[],
        description="F1 drivers - check Before Race for changes"
    ),

    "team_driver": TableConfig(
        name="team_driver",
        schema="formula_one",
        strategy=LoadStrategy.PRE_SEASON,
        api_endpoint="",
        dependencies=["driver", "team", "season"],
        description="Driver-team relationships per season"
    ),

    "sprint_result": TableConfig(
        name="sprint_result",
        schema="formula_one",
        strategy=LoadStrategy.POST_RACE,
        api_endpoint="/{year}/{round}/sprint.json",
        dependencies=["team", "round", "session"],
        description="Sprint result"
    ),

    "qualifying_result": TableConfig(
        name="qualifying_result",
        schema="formula_one",
        strategy=LoadStrategy.POST_RACE,
        api_endpoint="/{year}/{round}/qualifying.json",
        dependencies=["team", "round", "session"],
        description="Qualifying result"
    ),

    "race_result": TableConfig(
        name="sprint_result",
        schema="formula_one",
        strategy=LoadStrategy.POST_RACE,
        api_endpoint="/{year}/{round}/results.json",
        dependencies=["team", "round", "session"],
        description="Race result"
    ),

    "driver_championship": TableConfig(
        name="driver_championship",
        schema="formula_one",
        strategy=LoadStrategy.POST_RACE,
        api_endpoint="/{year}/{round}/driverStandings.json",
        dependencies=["driver", "round", "session"],
        description="Driver championship standings"
    ),

    "team_championship": TableConfig(
        name="team_championship",
        schema="formula_one",
        strategy=LoadStrategy.POST_RACE,
        api_endpoint="/{year}/{round}/constructorStandings.json",
        dependencies=["team", "round", "session"],
        description="Constructor championship standings"
    ),
}

# Loading order based on dependencies
LOAD_ORDER = [
    "circuit",
    "season",
    "team",
    "round",
    "session",
    "driver",
    "team_driver",
    "sprint_result",
    "qualifying_result",
    "race_result",
    "driver_championship",
    "team_championship",
]

# Mode definitions - what tables to load for each mode
LOAD_MODES = {
    "all": LOAD_ORDER,
    "pre_season": ["circuit", "season", "round", "session", "team", "driver", "team_driver"],
    "post_race": ["sprint_result", "qualifying_result", "race_result", "driver_championship", "team_championship"],
}

# Database configuration
load_dotenv()
USER = os.getenv("USER_SB")
PASSWORD = os.getenv("PASSWORD")
HOST = os.getenv("HOST")
PORT = os.getenv("PORT")
DBNAME = os.getenv("DBNAME")
SCHEMA = os.getenv("SCHEMA")
SCHEMA_METADATA = os.getenv("SCHEMA_METADATA")

# API configuration
JOLPICA_API_BASE = "https://api.jolpi.ca/ergast/f1"
API_TIMEOUT = 30
API_MAX_RETRIES = 3
API_RETRY_DELAY = 2

# Current season
CURRENT_SEASON = datetime.now().year

# Pipeline metadata
PIPELINE_VERSION = "1.0.0"
TRIGGERED_BY = os.getenv("GITHUB_ACTOR", "manual")
WORKFLOW_RUN_ID = os.getenv("GITHUB_RUN_ID", None)
