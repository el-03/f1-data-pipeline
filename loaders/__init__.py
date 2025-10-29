"""
loaders/__init__.py
All data loaders for F1 pipeline
"""
import zipfile
from abc import ABC
from html.entities import entitydefs
from typing import Any, List, Dict

from requests import session

from loaders.base_loader import BaseLoader
from infra.schema.schema_loader import SchemaLoader
import pandas as pd

from config import SCHEMA


class PreSeasonLoader(BaseLoader, ABC):
    def extract(self, **kwargs) -> Any:
        return self.api.get_raw_zip()

    def transform(self, raw_data: zipfile.ZipFile) -> Any:
        dump_list_csv = {
            "circuit": "formula_one_circuit.csv",
            "season": "formula_one_season.csv",
            "round": "formula_one_round.csv",
            "session": "formula_one_session.csv",
            "driver": "formula_one_driver.csv",
            "team": "formula_one_team.csv",
            "team_driver": "formula_one_teamdriver.csv"
        }

        dump_list_pd = {}

        for entity_name, csv_name in dump_list_csv.items():
            with raw_data.open(csv_name) as f:
                df = pd.read_csv(f)
                df = self.sanitize_df(df, entity_name)
                dump_list_pd[entity_name] = df

        return dump_list_pd

    def load(self, records: Dict[str, pd.DataFrame]) -> int:
        df = records[self.get_entity_name()]
        entity_name = self.get_entity_name()

        cur = self.conn.cursor()

        try:
            sql_count = f"""SELECT COUNT(*) FROM {SCHEMA}.{entity_name};"""
            cur.execute(sql_count)
            rows = cur.fetchall()

            count = len(df) - rows[0][0]
            if count > 0:
                cur.execute(f"""SELECT * FROM {SCHEMA}.{entity_name} LIMIT 1;""")
                columns = [desc[0] for desc in cur.description]
                diff_df = df[columns].tail(count)

                for _, row in diff_df.iterrows():
                    cols = ",".join(columns)
                    vals = ",".join(["%s"] * len(columns))
                    sql = f"INSERT INTO {SCHEMA}.{entity_name} ({cols}) VALUES ({vals}) ON CONFLICT (id) DO NOTHING;"
                    cur.execute(sql, tuple(row))

                # Reset ID sequence
                sql_reset_seq = f"SELECT setval('{SCHEMA}.{entity_name}_id_seq', (SELECT COALESCE(MAX(id), 0) FROM {SCHEMA}.{entity_name}));"
                cur.execute(sql_reset_seq)

            self.conn.commit()
            return count

        except Exception as e:
            self.conn.rollback()
            raise e
        finally:
            cur.close()

    @staticmethod
    def sanitize_df(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        schema = SchemaLoader.get_table_schema(table_name)
        df = df.copy()

        for col, dtype in schema.items():
            if col not in df.columns:
                continue

            # Convert based on JSON type
            if dtype in ("text", "varchar", "char"):
                df[col] = df[col].astype(str).replace({"nan": None, "NaT": None})
            elif dtype in ("integer", "smallint"):
                # Coerce to integer, clip if smallint
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
                if dtype == "smallint":
                    df[col] = df[col].clip(lower=-32768, upper=32767)
            elif dtype == "float":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)
            elif dtype == "boolean":
                df[col] = df[col].astype(str).str.lower().map({"true": True, "false": False, "t": True, "f": False})
            elif dtype == "date":
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
            elif dtype == "timestamp":
                df[col] = pd.to_datetime(df[col], errors="coerce")
            else:
                pass  # Unknown type — leave as-is

        return df


class CircuitLoader(PreSeasonLoader):
    def get_entity_name(self) -> str:
        return 'circuit'


class SeasonLoader(PreSeasonLoader):
    def get_entity_name(self) -> str:
        return 'season'


class RoundLoader(PreSeasonLoader):
    def get_entity_name(self) -> str:
        return 'round'


class SessionLoader(PreSeasonLoader):
    def get_entity_name(self) -> str:
        return 'session'


class DriverLoader(PreSeasonLoader):
    def get_entity_name(self) -> str:
        return 'driver'


class TeamLoader(PreSeasonLoader):
    def get_entity_name(self) -> str:
        return 'team'


class TeamDriverLoader(PreSeasonLoader):
    def get_entity_name(self) -> str:
        return 'team_driver'


class QualifyingResultLoader(BaseLoader):
    """Load qualifying results"""

    def get_entity_name(self) -> str:
        return "qualifying_result"

    def extract(self, year: int, round_num: int = None, **kwargs) -> Any:
        return self.api.get_qualifying_results(year, round_num)

    def transform(self, raw_data: Any) -> List[Dict]:
        race = raw_data.get("MRData", {}).get("RaceTable", {}).get("Races", [])
        if not race:
            return []

        race = race[0]
        season_year = int(race.get("season", 0))
        round_num = int(race.get("round", 0))

        lookup = self._build_lookup_maps(season_year, round_num, 'Q3')
        driver_map = lookup["driver_map"]
        team_map = lookup["team_map"]
        season_map = lookup["season_map"]
        round_map = lookup["round_map"]
        session_map = lookup["session_map"]

        records = []

        season_year = int(race.get("season", 0))
        round_num = int(race.get("round", 0))
        qualifying_results = race.get("QualifyingResults", [])

        season_id = season_map.get(season_year)
        round_id = round_map.get((season_year, round_num))

        session_id = session_map.get(round_id)['id']

        for result in qualifying_results:
            driver_ref = result.get("Driver", {}).get("driverId")
            constructor_ref = result.get("Constructor", {}).get("constructorId")

            driver_id = driver_map.get(driver_ref)
            team_id = team_map.get(constructor_ref)
            if not (driver_id and team_id):
                continue

            q1 = result.get("Q1")
            q2 = result.get("Q2")
            q3 = result.get("Q3")

            records.append({
                "season_id": season_id,
                "round_id": round_id,
                "last_session_id": session_id,
                "driver_id": driver_id,
                "team_id": team_id,
                "position": int(result.get("position", 0)),
                "q1_time": q1,
                "q1_time_milliseconds": self.convert_time_to_ms(q1),
                "q2_time": q2,
                "q2_time_milliseconds": self.convert_time_to_ms(q2),
                "q3_time": q3,
                "q3_time_milliseconds": self.convert_time_to_ms(q3),
            })

        return records

    def load(self, records: List[Dict]) -> int:
        cur = self.conn.cursor()
        count = 0
        try:
            for record in records:
                cur.execute(f"""
                    INSERT INTO {SCHEMA}.qualifying_result
                      (season_id, round_id, last_session_id, driver_id, team_id, position,
                       q1_time, q1_time_milliseconds,
                       q2_time, q2_time_milliseconds,
                       q3_time, q3_time_milliseconds)
                    VALUES (%(season_id)s, %(round_id)s, %(last_session_id)s, %(driver_id)s, %(team_id)s, %(position)s,
                            %(q1_time)s, %(q1_time_milliseconds)s,
                            %(q2_time)s, %(q2_time_milliseconds)s,
                            %(q3_time)s, %(q3_time_milliseconds)s)
                    ON CONFLICT (season_id, round_id, driver_id) DO UPDATE SET
                      position = EXCLUDED.position,
                      q1_time = EXCLUDED.q1_time,
                      q1_time_milliseconds = EXCLUDED.q1_time_milliseconds,
                      q2_time = EXCLUDED.q2_time,
                      q2_time_milliseconds = EXCLUDED.q2_time_milliseconds,
                      q3_time = EXCLUDED.q3_time,
                      q3_time_milliseconds = EXCLUDED.q3_time_milliseconds
                """, record)
                count += 1
            self.conn.commit()
            return count
        except Exception as e:
            self.conn.rollback()
            raise e
        finally:
            cur.close()


class SprintResultLoader(BaseLoader):
    """Load sprint race results"""

    def get_entity_name(self) -> str:
        return "sprint_result"

    def extract(self, year: int, round_num: int = None, **kwargs) -> Any:
        return self.api.get_sprint_results(year, round_num)

    def transform(self, raw_data: Any) -> List[Dict]:
        race = raw_data.get("MRData", {}).get("RaceTable", {}).get("Races", [])
        if not race:
            return []

        race = race[0]
        season_year = int(race.get("season", 0))
        round_num = int(race.get("round", 0))

        lookup = self._build_lookup_maps(season_year, round_num, 'SR')
        driver_map = lookup["driver_map"]
        team_map = lookup["team_map"]
        season_map = lookup["season_map"]
        round_map = lookup["round_map"]
        session_map = lookup["session_map"]

        records = []

        sprint_results = race.get("SprintResults", [])

        season_id = season_map.get(season_year)
        round_id = round_map.get((season_year, round_num))

        session_id = session_map.get(round_id)["id"]

        for result in sprint_results:
            driver_ref = result.get("Driver", {}).get("driverId")
            constructor_ref = result.get("Constructor", {}).get("constructorId")

            driver_id = driver_map.get(driver_ref)
            team_id = team_map.get(constructor_ref)
            if not (driver_id and team_id):
                continue

            records.append({
                "season_id": season_id,
                "round_id": round_id,
                "session_id": session_id,
                "driver_id": driver_id,
                "team_id": team_id,
                "position": int(result.get("position", 0)),
                "position_text": result.get("positionText"),
                "points": float(result.get("points", 0)),
                "grid_position": result.get("grid"),
                "laps_completed": result.get("laps", 0),
                "status": result.get("status"),
                "sprint_time_milliseconds": result.get("Time", {}).get("millis"),
            })

        return records

    def load(self, records: List[Dict]) -> int:
        cur = self.conn.cursor()
        count = 0
        try:
            for record in records:
                cur.execute(f"""
                    INSERT INTO {SCHEMA}.sprint_result
                      (season_id, round_id, session_id, driver_id, team_id, position, position_text,
                       points, grid_position, laps_completed, status, sprint_time_milliseconds)
                    VALUES (%(season_id)s, %(round_id)s, %(session_id)s, %(driver_id)s, %(team_id)s,
                            %(position)s, %(position_text)s,
                            %(points)s, %(grid_position)s, %(laps_completed)s, %(status)s, %(sprint_time_milliseconds)s)
                    ON CONFLICT (season_id, round_id, driver_id) DO UPDATE SET
                      position = EXCLUDED.position,
                      position_text = EXCLUDED.position_text,
                      points = EXCLUDED.points,
                      grid_position = EXCLUDED.grid_position,
                      laps_completed = EXCLUDED.laps_completed,
                      status = EXCLUDED.status,
                      sprint_time_milliseconds = EXCLUDED.sprint_time_milliseconds
                """, record)
                count += 1
            self.conn.commit()
            return count
        except Exception as e:
            self.conn.rollback()
            raise e
        finally:
            cur.close()


class RaceResultLoader(BaseLoader):
    """Load race results"""

    def get_entity_name(self) -> str:
        return "race_result"

    def extract(self, year: int, round_num: int = None, **kwargs) -> Any:
        return self.api.get_race_results(year, round_num)

    def transform(self, raw_data: Any) -> List[Dict]:
        race = raw_data.get("MRData", {}).get("RaceTable", {}).get("Races", [])
        if not race:
            return []

        race = race[0]
        season_year = int(race.get("season", 0))
        round_num = int(race.get("round", 0))

        lookup = self._build_lookup_maps(season_year, round_num, 'R')
        driver_map = lookup["driver_map"]
        team_map = lookup["team_map"]
        season_map = lookup["season_map"]
        round_map = lookup["round_map"]
        session_map = lookup["session_map"]

        records = []

        race_results = race.get("Results", [])

        season_id = season_map.get(season_year)
        round_id = round_map.get((season_year, round_num))
        session_id = session_map.get(round_id)["id"]
        session_num = session_map.get(round_id)["number"]

        for result in race_results:
            driver_ref = result.get("Driver", {}).get("driverId")
            constructor_ref = result.get("Constructor", {}).get("constructorId")

            driver_id = driver_map.get(driver_ref)
            team_id = team_map.get(constructor_ref)
            if not (driver_id and team_id):
                continue

            time_data = result.get("Time", {})
            fastest_lap = result.get("FastestLap", {})

            records.append({
                "season_id": season_id,
                "round_id": round_id,
                "session_id": session_id,
                "driver_id": driver_id,
                "team_id": team_id,
                "grid_position": int(result.get("grid", 0)),
                "position": int(result.get("position", 0)),
                "position_text": result.get("positionText"),
                "points": float(result.get("points", 0)),
                "laps_completed": int(result.get("laps", 0)),
                "status": result.get("status"),
                "race_time_milliseconds": self.safe_int(time_data.get("millis")),
                "fastest_lap_rank": self.safe_int(fastest_lap.get("rank")),
                "fastest_lap_number": self.safe_int(fastest_lap.get("lap")),
                "fastest_lap_time": fastest_lap.get("Time", {}).get("time"),
                "fastest_lap_milliseconds": self.convert_time_to_ms(fastest_lap.get("Time", {}).get("time")),
            })

        return records

    def load(self, records: List[Dict]) -> int:
        cur = self.conn.cursor()
        count = 0

        try:
            for record in records:
                cur.execute(f"""
                    INSERT INTO {SCHEMA}.race_result
                      (season_id, round_id, session_id, driver_id, team_id, grid_position,
                       position, position_text, points, laps_completed, status, race_time_milliseconds,
                       fastest_lap_rank, fastest_lap_number, fastest_lap_time, fastest_lap_milliseconds)
                    VALUES (%(season_id)s, %(round_id)s, %(session_id)s, %(driver_id)s, %(team_id)s,
                            %(grid_position)s, %(position)s, %(position_text)s, %(points)s, 
                            %(laps_completed)s, %(status)s,
                            %(race_time_milliseconds)s,
                            %(fastest_lap_rank)s, %(fastest_lap_number)s, %(fastest_lap_time)s, %(fastest_lap_milliseconds)s)
                    ON CONFLICT (season_id, round_id, driver_id) DO UPDATE SET
                      position = EXCLUDED.position,
                      position_text = EXCLUDED.position_text,
                      points = EXCLUDED.points,
                      laps_completed = EXCLUDED.laps_completed,
                      status = EXCLUDED.status,
                      race_time_milliseconds = EXCLUDED.race_time_milliseconds,
                      fastest_lap_rank = EXCLUDED.fastest_lap_rank,
                      fastest_lap_number = EXCLUDED.fastest_lap_number,
                      fastest_lap_time = EXCLUDED.fastest_lap_time,
                      fastest_lap_milliseconds = EXCLUDED.fastest_lap_milliseconds;
                """, record)
                count += 1

            self.conn.commit()
            return count

        except Exception as e:
            self.conn.rollback()
            raise e
        finally:
            cur.close()



class DriverChampionshipLoader(BaseLoader):
    """Load driver championship standings"""

    def get_entity_name(self) -> str:
        return "driver_championship"

    def extract(self, year: int, round_num: int = None, **kwargs) -> Any:
        return self.api.get_driver_standings(year, round_num)

    def transform(self, raw_data: Any) -> List[Dict]:
        standings_list = raw_data.get("MRData", {}).get("StandingsTable", {}).get("StandingsLists", [])[0]
        if not standings_list:
            return []

        season_year = int(standings_list.get("season", 0))
        round_num = int(standings_list.get("round", 0))

        lookup = self._build_lookup_maps(season_year, round_num)
        driver_map = lookup["driver_map"]
        season_map = lookup["season_map"]
        round_map = lookup["round_map"]
        session_map = lookup["session_map"]

        records = []

        driver_standings = standings_list.get("DriverStandings", [])

        season_id = season_map.get(season_year)
        round_id = round_map.get((season_year, round_num))
        session_id = session_map.get(round_id)['id']
        session_num = session_map.get(round_id)['number']

        for standing in driver_standings:
            driver_ref = standing.get("Driver", {}).get("driverId", "")

            driver_id = driver_map.get(driver_ref)

            records.append({
                "season_id": season_id,
                "round_id": round_id,
                "session_id": session_id,
                "driver_id": driver_id,
                "round_number": round_num,
                "session_number": session_num,
                "year": season_year,
                "position": int(standing.get("position", 0)),
                "points": float(standing.get("points", 0)),
                "win_count": int(standing.get("wins", 0)),
            })

        return records

    def load(self, records: List[Dict]) -> int:
        cur = self.conn.cursor()
        count = 0

        try:
            for record in records:
                cur.execute(f"""
                    INSERT INTO {SCHEMA}.driver_championship 
                      (season_id, round_id, session_id, driver_id, 
                       round_number, session_number, year, position, points, win_count)
                    VALUES (%(season_id)s, %(round_id)s, %(session_id)s, %(driver_id)s,
                            %(round_number)s, %(session_number)s, %(year)s, %(position)s, 
                            %(points)s, %(win_count)s)
                    ON CONFLICT (season_id, round_id, driver_id) DO UPDATE SET
                      position = EXCLUDED.position,
                      points = EXCLUDED.points,
                      win_count = EXCLUDED.win_count
                """, record)
                count += 1

            self.conn.commit()
            return count

        except Exception as e:
            self.conn.rollback()
            raise e
        finally:
            cur.close()


class TeamChampionshipLoader(BaseLoader):
    """Load team championship standings"""

    def get_entity_name(self) -> str:
        return "team_championship"

    def extract(self, year: int, round_num: int = None, **kwargs) -> Any:
        return self.api.get_constructor_standings(year, round_num)

    def transform(self, raw_data: Any) -> List[Dict]:
        standings_list = raw_data.get("MRData", {}).get("StandingsTable", {}).get("StandingsLists", [])[0]
        if not standings_list:
            return []

        season_year = int(standings_list.get("season", 0))
        round_num = int(standings_list.get("round", 0))

        lookup = self._build_lookup_maps(season_year, round_num)
        team_map = lookup["team_map"]
        season_map = lookup["season_map"]
        round_map = lookup["round_map"]
        session_map = lookup["session_map"]

        records = []

        season_id = season_map.get(season_year)
        round_id = round_map.get((season_year, round_num))
        session_id = session_map.get(round_id)['id']
        session_num = session_map.get(round_id)['number']

        constructor_standings = standings_list.get("ConstructorStandings", [])

        for standing in constructor_standings:
            constructor_ref = standing.get("Constructor", {}).get("constructorId")
            team_id = team_map.get(constructor_ref)

            records.append({
                "season_id": season_id,
                "round_id": round_id,
                "session_id": session_id,
                "team_id": team_id,
                "round_number": round_num,
                "session_number": session_num,
                "year": season_year,
                "position": int(standing.get("position", 0)),
                "points": float(standing.get("points", 0)),
                "win_count": int(standing.get("wins", 0)),
            })

        return records

    def load(self, records: List[Dict]) -> int:
        cur = self.conn.cursor()
        count = 0

        try:
            for record in records:
                cur.execute(f"""
                    INSERT INTO {SCHEMA}.team_championship
                      (season_id, round_id, session_id, team_id, 
                       round_number, session_number, year, position, points, win_count)
                    VALUES (%(season_id)s, %(round_id)s, %(session_id)s, %(team_id)s,
                            %(round_number)s, %(session_number)s, %(year)s, %(position)s,
                            %(points)s, %(win_count)s)
                    ON CONFLICT (season_id, round_id, team_id) DO UPDATE SET
                      position = EXCLUDED.position,
                      points = EXCLUDED.points,
                      win_count = EXCLUDED.win_count
                """, record)
                count += 1

            self.conn.commit()
            return count

        except Exception as e:
            self.conn.rollback()
            raise e
        finally:
            cur.close()
