from abstract.singleton import Singleton
from ..agent_states import DaJsonAgentState
from settings import AgentSettings

from langchain.agents import initialize_agent, AgentType, Tool
from langchain.memory import ConversationBufferMemory
from langgraph.graph import StateGraph
from langchain.chat_models import ChatOpenAI
from langchain_openai import ChatOpenAI as CH
from langchain.schema import HumanMessage, SystemMessage, Document, AIMessage
from langchain_community.vectorstores import FAISS
from langchain.embeddings import SentenceTransformerEmbeddings
from langchain.chains import RetrievalQA
from langchain_community.tools import DuckDuckGoSearchRun
from langgraph.types import Command

from langgraph.graph import StateGraph, END, MessagesState, START
from typing import TypedDict, Optional, Literal
from typing_extensions import TypedDict

import warnings
import os, json

from scripts.validate_rename_patch import validate_rename_patch

class DaJsonAgentBuilder(Singleton):
    def _setup(self):
        self.settings = AgentSettings()

    def create_da_json_agent(self):
        llm = self.settings.select_model()

        memory_da_json = ConversationBufferMemory(memory_key="chat_history")
        tools = []
        da_json_agent = initialize_agent(
            tools=tools,
            llm=llm,
            memory=memory_da_json,
            agent=AgentType.CONVERSATIONAL_REACT_DESCRIPTION,#CHAT_ZERO_SHOT_REACT_DESCRIPTION,
            # return_intermediate_steps=False,
            verbose=True,
            handle_parsing_errors=True
        )
        return da_json_agent

    def create_da_json_agent_node(self, da_json_agent):
        def da_json_agent_node(state: DaJsonAgentState) -> Command[Literal["da_json_validator"]]:
            da_json_prompt = self.settings.da_json_template
            da_json_instruction = self.settings.da_json_instruction

            if "messages" in state and state["messages"]:
                old_messages = state["messages"]
                request = state["messages"][-1].content
            else:
                old_messages = []
                print("(create_da_json_agent_node)", state["task"])
                request = da_json_prompt.format(task=state["task"])
                print("(create_da_json_agent_node)", request)

            response = da_json_agent.run(request)
            if isinstance(response, dict):
                result = response["output"]
            else:
                result = response#.replace('json','').replace('````','')
            print("(create_da_json_agent_node111)", result)
            return Command(
                update={
                    "result": result,
                    "messages": old_messages + [HumanMessage(content=result, name="Аналитик")]
                },
                goto="da_json_validator"
            )
        return da_json_agent_node

    def create_da_json_validator_node(self):
        llm_max = self.settings.select_model()
        def da_json_validator_node(state: DaJsonAgentState) -> Command[Literal["da_json_agent", END]]:
            da_json_validator_prompt = self.settings.da_json_validator_prompt
            return_node = "da_json_agent"
            if not "messages" in state or not state["messages"]:
                return Command(goto=return_node)

            prompt = da_json_validator_prompt.format(task=state["task"])
            system = SystemMessage(content=prompt)
            request = [system] + state["messages"]

            validator_msgs_fount = len(list(filter(lambda x: x.name == "Валидатор", state["messages"])))
            response = self.settings.call_llm(llm_max, request, "validator")
            
            result = response["instructions"]
            print(f"STATUS: {response['status']}\n\nJSON ANALYTIC VALIDATOR: {result}")

            if not isinstance(result, dict):
                result_json = json.loads(result)

            if not isinstance(state["task"], dict):
                task_json = json.loads(state["task"])

            json_status = validate_rename_patch(task_json, result_json)

            print(f"PATCH_VALIDATOR_STATUS:{json_status}")

            if json_status != "SUCCESS":
                response["status"] = "FAIL"

            if response["status"] == "SUCCESS" or validator_msgs_fount + 1 > 3:
                goto = END
            else:
                result += "\nПомимо этого, есть ряд важных ошибок, которые нужно исправить: \n" + json_status
                goto = return_node
            return Command(
                update={
                    "messages": state["messages"] + [HumanMessage(content=result, name="Валидатор")],
                },
                goto=goto
            )
        return da_json_validator_node
