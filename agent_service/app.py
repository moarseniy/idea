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
            print(f"REQUEST(app.py): {request}")
            agents = LangchainApp()
            response = dict()

            if not request.needFix:
                task = request.task

                if request.darchRequirements:
                    darch_initial_state = {"task": task}
                    _, darch_response = await agents.darch_agent(darch_initial_state)
                    response["darchRequirements"] = darch_response
                
                if request.deRequirements:
                    de_initial_state = {"task": task}
                    _, de_response = await agents.de_agent(de_initial_state)
                    response["deRequirements"] = de_response
                
                if request.daRequirements:
                    da_initial_state = {"task": task}
                    _, da_response = await agents.da_agent(da_initial_state)
                    print("(app.py)", type(da_response), da_response)
                    response["daRequirements"] = da_response
                
                if request.daJsonRequirements:
                    da_json_initial_state = {"task": task}
                    _, da_json_response = await agents.da_json_agent(da_json_initial_state)
                    print("(app.py)", type(da_json_response), da_json_response)
                    response["daJsonRequirements"] = da_json_response

            else:

                if request.darchRequirements:
                    print("BBBBBBBBBBBBBBBBBBB", request.history, request.task)
                    result = await agents.darch_corrector_agent(history=request.history, task=request.task)
                    print("CCCCCCCCCCCCCCCCCCC", request.task)
                    response["darchRequirements"] = result

                if request.deRequirements:
                    result = await agents.de_corrector_agent(history=request.history, task=request.task)
                    response["deRequirements"] = result
                

            return AgentResponse(**response)

if __name__=="__main__":
    app = App()
    uvicorn.run(app, host="0.0.0.0", port=7861, log_level="info")
