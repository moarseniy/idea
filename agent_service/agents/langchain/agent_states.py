from typing import TypedDict, List

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
    de_requirements: str
    darch_requirements: str