from pydantic import BaseModel, computed_field
from typing import List, Dict, Optional
from datetime import datetime

class AgentRequest(BaseModel):
    deRequirements: Optional[bool] = None
    darchRequirements: Optional[bool] = None
    needFix: Optional[bool] = None
    jsonAgentHistory: Optional[List] = None
    history: Optional[Dict] = None
    task: Optional[str] = None
    requestDateTime: datetime

class AgentResponse(BaseModel):
    deRequirements: Optional[str] = None
    darchRequirements: Optional[str] = None
    message: Optional[str] = None
    history: Optional[Dict] = None

    @computed_field(return_type=datetime)
    def responseDateTime(self):
        return datetime.now()

    class Config:
        orm_mode = True
        exclude_none = True