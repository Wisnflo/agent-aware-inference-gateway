"""Benchmark workloads.

The sequential workload is byte-for-byte the Phase 2.1 capacity workload
(15 long, distinct session prefixes x 4 turns) so the gateway numbers can be
compared with the direct-backend baseline without changing the prompts.

The shuffled workload uses the same 60 requests with a fixed seed.  The
negative control gives every request a unique prefix, so session affinity
cannot produce cache reuse.
"""
from __future__ import annotations

import random
from dataclasses import dataclass

SEED = 20261102
TURNS = [
    "Turn one: propose an outline and one clarification question.",
    "Turn two: adjust for a rainy afternoon.",
    "Turn three: add a museum option and check constraints.",
    "Turn four: summarize and estimate the budget.",
]

SESSIONS = []
for index in range(15):
    unique = (
        "Session %d has a unique long policy for region %d, budget tier %d, "
        "accessibility profile %d, and preferred activity mix %d. "
        % (index, index, index % 4, index % 2, index % 3)
    )
    SESSIONS.append(unique * 14)

_WORDS = (
    "museum transit budget hotel district gallery station forecast ticket "
    "walking family stroller luggage reservation morning evening market "
    "harbor bridge castle garden cafe allergy timetable platform transfer "
    "weather indoor outdoor guide local currency receipt refund schedule "
    "arrival departure minute kilometer quiet lively central riverside "
    "hillside landmark exhibit workshop tasting picnic pharmacy clinic "
    "charging locker umbrella jacket camera battery backup optional"
).split()


@dataclass(frozen=True)
class WorkloadRequest:
    session: str
    turn: int
    prompt: str
    request_id: str


def sequential_requests() -> list[WorkloadRequest]:
    requests = []
    for turn_index, turn in enumerate(TURNS):
        for session_index, prefix in enumerate(SESSIONS):
            requests.append(
                WorkloadRequest(
                    session=f"session-{session_index}",
                    turn=turn_index,
                    prompt=prefix + " " + turn,
                    request_id=f"s{session_index}-t{turn_index}",
                )
            )
    return requests


def shuffled_requests() -> list[WorkloadRequest]:
    requests = sequential_requests()
    random.Random(SEED).shuffle(requests)
    return requests


def negative_requests() -> list[WorkloadRequest]:
    requests = []
    for session_index in range(15):
        for turn_index in range(4):
            rng = random.Random(9000 + session_index * 4 + turn_index)
            body = " ".join(rng.choice(_WORDS) for _ in range(420))
            requests.append(
                WorkloadRequest(
                    session=f"session-{session_index}",
                    turn=turn_index,
                    prompt=f"Unique request {session_index}-{turn_index}: {body}",
                    request_id=f"n{session_index}-t{turn_index}",
                )
            )
    return requests


WORKLOADS = {
    "sequential": (
        sequential_requests,
        "Phase 2.1 capacity workload: 15 sessions x 4 turns, turn-major order.",
    ),
    "shuffled": (
        shuffled_requests,
        f"Same 60 requests shuffled with random.Random({SEED}).",
    ),
    "negative": (
        negative_requests,
        "15 sessions x 4 unique-prefix requests; affinity cannot reuse cache.",
    ),
}


def build_workload(name: str) -> list[WorkloadRequest]:
    if name not in WORKLOADS:
        raise ValueError(f"unknown workload {name!r}; choose from {sorted(WORKLOADS)}")
    return WORKLOADS[name][0]()
