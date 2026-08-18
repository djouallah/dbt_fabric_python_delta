# Ask the published aemo_nem_agent the PLAN.md questions -- plain requests, like gql.py.
#
# No SDK and no asyncio. The agent's endpoint speaks JSON-RPC over ordinary HTTP POSTs, so
# four posts (initialize / initialized / tools/list / tools/call) are the whole client.
# The older REST route -- the OpenAI Assistants threads+runs endpoint under
# /aiskills/{id}/aiassistant/openai -- is deprecated with a shutdown date of 2026-08-26,
# so this is the one worth writing against.
#
# Three things about this endpoint that will otherwise waste an afternoon:
#   * the path segment is lowercase `dataagents`, while the management API path used to
#     look the agent up is `dataAgents`. Both spellings below are deliberate.
#   * the agent must have been PUBLISHED. An unpublished agent errors on a correct URL.
#   * Accept must list BOTH application/json and text/event-stream, and replies come back
#     either way -- an SSE reply is JSON hiding behind `data:` lines.
#
# Every wrong answer is an instruction gap: read the generated GQL in the portal's "steps"
# view, extend INSTRUCTIONS in data_agent.py, redeploy, re-ask.

import json
import os
import sys
import time

import requests
from duckrun.auth import get_fabric_token

WORKSPACE = os.environ.get("WS_ID", "450bf196-431f-463f-9316-2d1ce1da98db")  # sqlengines
AGENT     = "aemo_nem_agent"
API       = "https://api.fabric.microsoft.com/v1"
TIMEOUT   = 600  # seconds; the ~37s full rollup traversal is only part of one turn

# (question, expected) -- the expectation is PRINTED, never sent. Handing the agent the
# answer would make this a spelling test. Ground truth comes from gql.py and from direct
# GQL against the same graph; the same numbers fall out of a recursive CTE over the Delta
# tables. The last three are measure questions, which is where every silent-wrong-number
# bug showed up: 556,218 MWh meant the 5/60 factor was dropped, and 2,076,155 readings
# meant a fan-out. Both were reported confidently as fact, which is why the scope counts
# are part of the expectation and not just the total.
QUESTIONS = [
    ("If Basslink goes out of service, which participants lose all access to the mainland "
     "market? List the participants.",
     "TAS1 is islanded (Basslink is its only in-service link); 4-5 participants"),

    ("Which regions can Tasmanian generation reach within two in-service interconnector "
     "hops?",
     "VIC1 at one hop; SA1 and NSW1 at two"),

    ("What is AGL's total registered capacity across all regions, including its "
     "subsidiaries?",
     "41 units / 8850.6 MW"),

    ("Which stations have both a generating unit and a load unit, and who owns them?",
     "3 stations: Shoalhaven, Kennedy Energy Park Battery, Wivenhoe"),

    ("Which South Australian generators share an owner with a Victorian generator?",
     "Snowy Hydro, Origin Energy, EnergyAustralia Yallourn"),

    ("What are the top 10 stations by registered capacity?",
     "Eraring 2921.5 MW, Bayswater 2640 MW, Loy Yang A 2210 MW, ..."),

    # v5 added RegionInterval, so the regional price is now ONE authoritative value per
    # interval instead of the same number copied onto every unit. Note the expectation
    # CHANGED: $35.665 is the true time-average over 288 intervals. The $35.145 this file
    # used to expect was the unit-weighted average over 15,032 unit-rows -- the right answer
    # to a question nobody asked, which is all v4 could compute.
    ("What was the average spot price in SA1 on 2026-08-09?",
     "$35.665/MWh over 288 intervals, from RegionInterval (NOT the unit-weighted $35.145)"),

    ("What was the total generation in MWh in SA1 on 2026-08-09?",
     "46,351.5 MWh from 15,032 readings across 66 units"),

    ("What was the average demand in Queensland last week, and its peak?",
     "avg 5,916 MW, peak 8,072 MW for the week ending 2026-08-11"),

    ("Is Queensland a net importer or exporter of electricity, and by how much?",
     "net EXPORTER, about +797 MW on average"),

    ("How much power flowed between each pair of regions last week?",
     "QLD1-NSW1 -797, VIC1-NSW1 +444, TAS1-VIC1 -125, SA1-VIC1 -33 MW average"),

    ("How much spare generating capacity did South Australia have last week?",
     "AvailableGeneration - TotalDemand, about 2,042 MW average"),
]

session = requests.Session()
session.headers.update({
    "Authorization": f"Bearer {get_fabric_token()}",
    "Content-Type": "application/json",
    # Both, always: the server picks which one it answers with.
    "Accept": "application/json, text/event-stream",
})

agents = session.get(f"{API}/workspaces/{WORKSPACE}/dataAgents").json()["value"]
agent = next((a for a in agents if a["displayName"] == AGENT), None)
if not agent:
    raise SystemExit(f"No data agent named '{AGENT}'. Found: "
                     f"{[a['displayName'] for a in agents]}. Run data_agent.py first.")
URL = f"{API}/mcp/workspaces/{WORKSPACE}/dataagents/{agent['id']}/agent"


def rpc(method, params=None, notify=False):
    """One JSON-RPC POST. Returns the `result` object, or None for a notification."""
    body = {"jsonrpc": "2.0", "method": method}
    if params is not None:
        body["params"] = params
    if not notify:
        rpc.seq += 1
        body["id"] = rpc.seq

    resp = session.post(URL, json=body, timeout=TIMEOUT)
    resp.raise_for_status()
    # The session id is minted by initialize and must be echoed on every later call.
    if "mcp-session-id" in resp.headers:
        session.headers["Mcp-Session-Id"] = resp.headers["mcp-session-id"]
    if notify or not resp.content:
        return None

    if resp.headers.get("Content-Type", "").startswith("text/event-stream"):
        payloads = [json.loads(line[5:]) for line in resp.text.splitlines()
                    if line.startswith("data:")]
        message = next((p for p in payloads if p.get("id") == body.get("id")), payloads[-1])
    else:
        message = resp.json()

    if "error" in message:
        raise RuntimeError(f"{method}: {message['error']}")
    return message["result"]


rpc.seq = 0

rpc("initialize", {"protocolVersion": "2025-06-18", "capabilities": {},
                   "clientInfo": {"name": "ask.py", "version": "1"}})
session.headers["MCP-Protocol-Version"] = "2025-06-18"
rpc("notifications/initialized", notify=True)

# The data agent exposes exactly one tool. Discover it rather than hard-coding its name or
# the name of its question argument.
tool = rpc("tools/list", {})["tools"][0]
arg = next(iter(tool["inputSchema"]["properties"]))
print(f"agent: {AGENT} ({agent['id']})\ntool:  {tool['name']}({arg})\n")


def ask(question):
    result = rpc("tools/call", {"name": tool["name"], "arguments": {arg: question}})
    return "\n".join(c.get("text", "") for c in result.get("content", [])).strip()


# The agent is NOT deterministic: the same question against the same published agent can
# answer correctly on one run and report a "technical issue" on the next. So each question
# gets two attempts, and the retry is printed rather than hidden -- a question that needs
# retrying is a real quality signal, not noise to be smoothed away.
DUD = ("technical issue", "technical query error", "technical problem", "unable to",
       "no data was found", "was not able", "failed due to", "not found in the available",
       "could not find", "no entry specifically")

asked = [(q, None) for q in sys.argv[1:]] or QUESTIONS

for n, (question, expected) in enumerate(asked):
    # Space the questions out. Asking back to back reliably degrades: heavy Observation
    # aggregations that answer correctly in isolation start returning 500s and "technical
    # issue" prose once several have run in a row. Structural questions survive it; measure
    # questions do not.
    if n:
        time.sleep(20)
    print("=" * 70)
    print(question)
    if expected:
        print(f"    expected: {expected}")
    answer = ""
    for attempt in (1, 2):
        try:
            answer = ask(question)
        except Exception as exc:  # noqa: BLE001 -- one bad turn shouldn't end the run
            answer = ""
            print(f"    attempt {attempt}: {type(exc).__name__}: {str(exc)[:120]}")
        if answer and not any(d in answer.lower() for d in DUD):
            break
        if attempt == 1:
            print("    attempt 1 came back empty or self-reported a failure, retrying")
            time.sleep(15)
    print(f"\n{answer or '    (no answer)'}\n")
