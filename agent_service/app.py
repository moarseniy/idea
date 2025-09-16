import asyncio

from api.agent_api import AgentRequest, AgentResponse
from agents.langchain.agent_app import LangchainApp

from fastapi import FastAPI
import uvicorn

class App(FastAPI):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.setup_endpoints()

    def setup_endpoints(self):
        @self.post("/llm_agents")
        async def agent_endpoint(request:AgentRequest)->AgentResponse:
            print(f"REQUEST: {request}")
            agents = LangchainApp(memory_json=request.jsonAgentHistory)
            response = dict()
            if not request.needFix:
                task = request.task
                if request.businessRequirements or request.systemRequirements:
                    ba_initial_state = {"task":task}
                    result = await agents.ba_agent(ba_initial_state)
                    _, businessRequirements = result
                    if request.businessRequirements:
                        response["businessRequirements"] = businessRequirements
                if request.systemRequirements:
                    sa_initial_state = {"task": task, "ba_requirements": businessRequirements}
                    _, sa_requirements = await agents.sa_agent(sa_initial_state)
                    response["systemRequirements"] = sa_requirements
                if request.bpmnSchema:
                    bpmn_initial_state = {"task": task}
                    _, bpmn_schema = await agents.bpmn_agent(bpmn_initial_state)
                    response["bpmnSchema"] = bpmn_schema
                if request.jsonSchema:
                    result = await agents.json_analitic_agent(request.task)
                    if "question" in result:
                        response["message"] = result["question"]
                        response["needInfo"] = True
                        response["jsonAgentHistory"] = result["history"]
                    if "json" in result and result["json"]:
                        response["jsonSchema"] = result["json"]

            else:
                if request.jsonSchema:
                    result = await agents.json_corrector_agent(history=request.history, task=request.task)
                    if "status" in result and result["status"]=="FAIL":
                        response["message"] = result["result"]
                        response["jsonSchema"] = request.history["jsonSchema"]
                    else:
                        response["jsonSchema"] = result["result"]
                if request.businessRequirements:
                    result = await agents.ba_corrector_agent(history=request.history, task=request.task)
                    response["businessRequirements"] = result
                if request.systemRequirements:
                    result = await agents.sa_corrector_agent(history=request.history, task=request.task)
                    response["systemRequirements"] = result
                if request.bpmnSchema:
                    result = await agents.bpmn_corrector_agent(history=request.history, task=request.task)
                    response["bpmnSchema"] = result
            return AgentResponse(**response)

if __name__=="__main__":
    app = App()
    uvicorn.run(app, host="0.0.0.0", port=5000, log_level="info")