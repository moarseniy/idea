from typing import TypedDict, Literal
from pydantic import BaseModel, Field

class ValidatorResponse(TypedDict):
    """В поле instructions нужно указать рекомендации. В поле status указать статус задачи [FAIL, SUCCESS]"""
    instructions: str
    status: Literal["FAIL", "SUCCESS"]

class OrchestratorResponse(TypedDict):
    next: Literal["da_agent", "da_json_agent", "de_agent", "darch_agent"]

class JsonValidatorResponse(TypedDict):
    """В поле result нужно указать результат доработки. В поле status указать статус проверки [FAIL, SUCCESS]"""
    result: str
    status: Literal["FAIL", "SUCCESS"]

responses_types = {"validator": ValidatorResponse, 
                    "orchestrator": OrchestratorResponse,
                    "json_validator": JsonValidatorResponse}

from pydantic import BaseModel, Field

class HFValidatorResponse(BaseModel):
    instructions: str = Field(..., description="Рекомендации по доработке")
    status: Literal["FAIL", "SUCCESS"] = Field(..., description="Статус задачи")

class HFOrchestratorResponse(BaseModel):
    next: Literal["da_agent", "da_json_agent", "de_agent", "darch_agent"] = Field(..., description="Следующий агент")

class HFJsonValidatorResponse(BaseModel):
    result: str = Field(..., description="Результат доработки")
    status: Literal["FAIL", "SUCCESS"] = Field(..., description="Статус проверки")


hf_responses_types = {"validator": HFValidatorResponse, 
                        "orchestrator": HFOrchestratorResponse,
                        "json_validator": HFJsonValidatorResponse}