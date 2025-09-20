from abstract.singleton import Singleton
from ..agent_states import DarchAgentState
from settings import AgentSettings

from langchain.agents import initialize_agent, AgentType, Tool
from langchain.memory import ConversationBufferMemory
from langgraph.graph import StateGraph
from langchain.chat_models import ChatOpenAI
from langchain_openai import ChatOpenAI as CH
from langchain.schema import HumanMessage, SystemMessage, Document, AIMessage
from langchain.vectorstores import FAISS
from langchain.embeddings import SentenceTransformerEmbeddings
from langchain.chains import RetrievalQA
from langchain_community.tools import DuckDuckGoSearchRun
from langgraph.types import Command

from langgraph.graph import StateGraph, END, MessagesState, START
from typing import TypedDict, Optional, Literal
from typing_extensions import TypedDict

import warnings
import os

class DarchAgentBuilder(Singleton):
    def _setup(self):
        self.settings = AgentSettings()

    def create_darch_agent(self):
        llm = self.settings.select_model()

        memory_darch = ConversationBufferMemory(memory_key="chat_history")
        tools = []
        darch_agent = initialize_agent(
            tools=tools,
            llm=llm,
            memory=memory_darch,
            agent=AgentType.CONVERSATIONAL_REACT_DESCRIPTION,
            verbose=True,
            handle_parsing_errors=True
        )
        return darch_agent

    def create_darch_agent_node(self, darch_agent):
        def darch_agent_node(state: DarchAgentState) -> Command[Literal["darch_validator"]]:
            darch_prompt = self.settings.darch_template
            darch_instruction = self.settings.darch_instruction

            if "messages" in state and state["messages"]:
                old_messages = state["messages"]
                request = state["messages"][-1].content
            else:
                old_messages = []
                request = darch_prompt.format(task=state["task"], darch_instruction=darch_instruction)

            response = darch_agent.run(request)
            if isinstance(response, dict):
                result = response["output"]
            else:
                result = response
            return Command(
                update={
                    "result": result,
                    "messages": old_messages + [HumanMessage(content=result, name="Архитектор")]
                },
                goto="darch_validator"
            )
        return darch_agent_node

    def create_darch_validator_node(self):
        llm_max = self.settings.select_model()
        def darch_validator_node(state: DarchAgentState) -> Command[Literal["darch_agent", END]]:
            darch_validator_prompt = self.settings.darch_validator_prompt
            return_node = "darch_agent"
            if not "messages" in state or not state["messages"]:
                return Command(goto=return_node)

            prompt = darch_validator_prompt.format(task=state["task"])
            system = SystemMessage(content=prompt)
            request = [system] + state["messages"]

            validator_msgs_fount = len(list(filter(lambda x: x.name == "Валидатор", state["messages"])))
            response = self.settings.call_llm(llm_max, request, "validator")
            
            result = response["instructions"]
            print(f"STATUS: {response['status']}\n\nVALIDATOR: {result}")

            if response["status"] == "SUCCESS" or validator_msgs_fount + 1 > 3:
                goto = END
            else:
                goto = return_node
            return Command(
                update={
                    "messages": state["messages"] + [HumanMessage(content=result, name="Валидатор")],
                },
                goto=goto
            )
        return darch_validator_node
