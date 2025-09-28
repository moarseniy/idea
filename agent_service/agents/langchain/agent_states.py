from typing import TypedDict, List

class DaAgentState(TypedDict):
    task: str
    messages: List
    result: str

class DeAgentState(TypedDict):
    task: str
    messages: List
    result: str

class DarchAgentState(TypedDict):
    task: str
    messages: List
    result: str

class CorrectorAgentState(TypedDict):
    task: str
    prev_context: str
    messages: List
    da_requirements: str
    de_requirements: str
    darch_requirements: str