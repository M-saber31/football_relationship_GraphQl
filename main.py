import os
from fastapi import FastAPI
from ariadne import QueryType, make_executable_schema, gql
from ariadne.asgi import GraphQL
from neo4j import GraphDatabase
import json

# Load schema
type_defs = gql(open("schema.graphql").read())
query = QueryType()

# --- Neo4j Connection from Environment Variables ---
NEO4J_URI = os.getenv("NEO4J_URI", "neo4j+ssc://43be5da7.databases.neo4j.io")
NEO4J_USER = os.getenv("NEO4J_USER", "43be5da7")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "vaguOzp9nJdSR_P_XUO3N05Ut0-SToIj8H0ZKaIXjiU")

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

# --- Resolvers ---
@query.field("player")
def resolve_player(_, info, name):
    with driver.session() as session:
        result = session.run("""
            MATCH (p:Player {name:$name})
            RETURN p.name AS name
        """, {"name": name}).single()
        return {"name": result["name"]} if result else None


@query.field("teammates")
def resolve_teammates(_, info, name, sortBy=None ,club=None):
    with driver.session() as session:
        results = session.run("""
            MATCH (p:Player {name:$name})-[r:PLAYED_TOGETHER]-(t:Player)
            RETURN t.name AS teammate, r.history AS history
        """, {"name": name})

        teammate_map = {}

        for record in results:
            teammate_name = record["teammate"]
            history_raw = record["history"]

            history = {}
            if history_raw:
                try:
                    history = json.loads(history_raw)
                except Exception:
                    history = {}

            if teammate_name not in teammate_map:
                teammate_map[teammate_name] = {
                    "player": {"name": teammate_name},
                    "clubs": [],
                    "season_count": 0
                }

            for c, seasons in history.items():
                if club and c != club:  # ✅ filter by club name
                    continue
                teammate_map[teammate_name]["clubs"].append({
                    "club": {"name": c},
                    "seasons": seasons
                })
                teammate_map[teammate_name]["season_count"] += len(seasons)

        teammates = list(teammate_map.values())
        if sortBy == "seasons":
            teammates = sorted(teammates, key=lambda x: x["season_count"], reverse=True)

        for t in teammates:
            t.pop("season_count", None)

        # ✅ Remove players with no club matches (when club filter is applied)
        if club:
            teammates = [t for t in teammates if t["clubs"]]

        return teammates


@query.field("commonTeammates")
def resolve_common_teammates(_, info, players):
    if not players or len(players) < 2:
        return []

    with driver.session() as session:
        results = session.run("""
            MATCH (t:Player)
            WHERE ALL(name IN $players WHERE (t)-[:PLAYED_TOGETHER]-(:Player {name:name}))
            WITH t
            MATCH (p:Player)-[r:PLAYED_TOGETHER]-(t)
            WHERE p.name IN $players
            RETURN t.name AS teammate, p.name AS withPlayer, r.history AS history
        """, {"players": players})

        teammate_map = {}

        for record in results:
            teammate_name = record["teammate"]
            with_player = record["withPlayer"]
            history_raw = record["history"]

            history = {}
            if history_raw:
                try:
                    history = json.loads(history_raw)
                except Exception:
                    history = {}

            if teammate_name not in teammate_map:
                teammate_map[teammate_name] = {
                    "player": {"name": teammate_name},
                    "clubs": {}
                }

            for club, seasons in history.items():
                if club not in teammate_map[teammate_name]["clubs"]:
                    teammate_map[teammate_name]["clubs"][club] = {
                        "club": {"name": club},
                        "seasons": set(seasons),
                        "withPlayers": set([with_player])
                    }
                else:
                    teammate_map[teammate_name]["clubs"][club]["seasons"] |= set(seasons)
                    teammate_map[teammate_name]["clubs"][club]["withPlayers"].add(with_player)

        # Convert dicts/sets into GraphQL-friendly lists
        output = []
        for teammate_name, data in teammate_map.items():
            clubs = []
            for club_data in data["clubs"].values():
                clubs.append({
                    "club": club_data["club"],
                    "seasons": sorted(list(club_data["seasons"])),
                    "withPlayers": [{"name": p} for p in sorted(list(club_data["withPlayers"]))]
                })
            output.append({
                "player": data["player"],
                "clubs": clubs
            })

        return output
# Build schema
schema = make_executable_schema(type_defs, query)

# FastAPI app
app = FastAPI()

@app.get("/ping")
def health_check():
    try:
        with driver.session() as session:
            session.run("RETURN 1")
        return {"status": "ok", "db": "connected"}
    except Exception as e:
        return {"status": "error", "db": str(e)}


app.mount("/graphql", GraphQL(schema, debug=True))