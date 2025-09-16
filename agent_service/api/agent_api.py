from pydantic import BaseModel, computed_field
from typing import List, Dict, Optional
from datetime import datetime

class AgentRequest(BaseModel):
    systemRequirements: Optional[bool] = None
    jsonSchema: Optional[bool] = None
    businessRequirements: Optional[bool] = None
    bpmnSchema: Optional[bool] = None
    needFix: Optional[bool] = None
    needInfo: Optional[bool] = None
    jsonAgentHistory: Optional[List] = None
    history: Optional[Dict] = None
    task: Optional[str] = None
    requestDateTime: datetime

class AgentResponse(BaseModel):
    systemRequirements: Optional[str] = None
    jsonSchema: Optional[str] = None
    businessRequirements: Optional[str] = None
    bpmnSchema: Optional[str] = None
    needInfo: Optional[bool] = None
    message: Optional[str] = None
    history: Optional[Dict] = None
    jsonAgentHistory: Optional[List] = None

    @computed_field(return_type=datetime)
    def responseDateTime(self):
        return datetime.now()

    class Config:
        orm_mode = True
        exclude_none = True