from typing import TypedDict, List

class BaAgentState(TypedDict):
    task: str
    messages: List
    result: str

class SaAgentState(TypedDict):
    messages: List
    task: str
    elements: str
    ba_requirements: str
    sa_requirements: str
    recommendations: str
    result: str

class BpmnAgentState(TypedDict):
    task: str
    messages: List
    description: str
    bpmn: str

class CorrectorAgentState(TypedDict):
    messages: List
    ba_requirements: str
    bpmn: str
    sa_requirements: str
    json_schema: str