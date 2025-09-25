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

            else:

                if request.darchRequirements:
                    print("BBBBBBBBBBBBBBBBBBB")
                    result = await agents.darch_corrector_agent(history=request.history, task=request.task)
                    print("CCCCCCCCCCCCCCCCCCC")
                    response["darchRequirements"] = result

                if request.deRequirements:
                    result = await agents.de_corrector_agent(history=request.history, task=request.task)
                    response["deRequirements"] = result
                

            return AgentResponse(**response)

if __name__=="__main__":
    app = App()
    uvicorn.run(app, host="0.0.0.0", port=7861, log_level="info")
