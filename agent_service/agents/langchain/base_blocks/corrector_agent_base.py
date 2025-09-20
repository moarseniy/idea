from abstract.singleton import Singleton
from ..agent_states import CorrectorAgentState
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
import os

class CorrectionAgentBuilder(Singleton):
    def _setup(self):
        self.settings = AgentSettings()

    def create_de_corrector_agent(self):
        llm = self.settings.select_model()

        memory_de = ConversationBufferMemory(memory_key="chat_history")
        tools = []
        de_agent = initialize_agent(
            tools=tools,
            llm=llm,
            memory=memory_de,
            agent=AgentType.CONVERSATIONAL_REACT_DESCRIPTION,
            verbose=True,
            handle_parsing_errors=True
        )
        return de_agent

    def create_darch_corrector_agent(self):
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

    def create_correction_orchestrator_node(self):
        llm = self.settings.select_model()

        def orchestrator_node(state: CorrectorAgentState) -> Command[Literal["de_agent", "darch_agent"]]:
            orchestrator_prompt = self.settings.correction_orchestrator_prompt
            request = [SystemMessage(content=orchestrator_prompt)] + state["messages"]
            response = self.settings.call_llm(llm, request, "orchestrator")
            goto = response["next"]
            return Command(goto=goto)
        return orchestrator_node

    def create_correction_de_node(self, de_corrector_agent):
        def de_requirements_node(state: CorrectorAgentState) -> Command[Literal["de_validator"]]:
            task = state["task"]
            prev_context = state["prev_context"]
            de_correction_prompt = self.settings.correction_de_agent_prompt.format(task=task, prev_context=prev_context)
            if "messages" in state and state["messages"]:
                old_messages = state["messages"]
                request = state["messages"][-1].content
            else:
                old_messages = []
                request = de_correction_prompt
            response = de_corrector_agent.run(request)
            if isinstance(response, dict):
                result = response["output"]
            else:
                result = response
            goto = "de_validator"
            return Command(
                update={
                    "de_requirements": result,
                    "messages": old_messages + [HumanMessage(content=result, name="Инженер")]
                },
                goto=goto
            )
        return de_requirements_node

    def create_correction_de_validator_node(self):
        llm = self.settings.select_model()

        def de_validator_node(state: CorrectorAgentState) -> Command[Literal["de_agent", END]]:
            return_node = "de_agent"
            task = state["task"]
            prev_context = state["prev_context"]
            de_validator_prompt = self.settings.correction_de_validator_agent_prompt.format(task=task, prev_context=prev_context)
            if "messages" in state and state["messages"]:
                system_prompt = de_validator_prompt
                request = [SystemMessage(content=system_prompt)] + state["messages"]
                old_messages = state["messages"]
            else:
                return Command(goto=return_node)
            validator_msgs_fount = len(list(filter(lambda x: x.name == "Валидатор", state["messages"])))
            response = self.settings.call_llm(llm, request, "validator")
            result = response["instructions"]

            if response["status"] == "SUCCESS" or validator_msgs_fount + 1 > 3:
                goto = END
            else:
                goto = return_node
            print(f"STATUS: {response['status']}\n\nVALIDATOR: {result}")
            return Command(
                update={
                    "messages": old_messages + [HumanMessage(content=result, name="Валидатор")],
                },
                goto=goto
            )
        return de_validator_node

    def create_correction_darch_node(self, darch_corrector_agent):
        def darch_requirements_node(state: CorrectorAgentState) -> Command[Literal["darch_validator"]]:
            task = state["task"]
            prev_context = state["prev_context"]
            darch_correction_prompt = self.settings.correction_darch_agent_prompt.format(task=task, prev_context=prev_context)
            if "messages" in state and state["messages"]:
                old_messages = state["messages"]
                request = state["messages"][-1].content
            else:
                old_messages = []
                request = darch_correction_prompt
            response = darch_corrector_agent.run(request)
            if isinstance(response, dict):
                result = response["output"]
            else:
                result = response
            goto = "darch_validator"
            return Command(
                update={
                    "darch_requirements": result,
                    "messages": old_messages + [HumanMessage(content=result, name="Архитектор")]
                },
                goto=goto
            )
        return darch_requirements_node

    def create_correction_darch_validator_node(self):
        llm = self.settings.select_model()

        def darch_validator_node(state: CorrectorAgentState) -> Command[Literal["darch_agent", END]]:
            return_node = "darch_agent"
            task = state["task"]
            prev_context = state["prev_context"]
            darch_validator_prompt = self.settings.correction_darch_validator_agent_prompt.format(task=task, prev_context=prev_context)
            if "messages" in state and state["messages"]:
                system_prompt = darch_validator_prompt
                request = [SystemMessage(content=system_prompt)] + state["messages"]
                old_messages = state["messages"]
            else:
                return Command(goto=return_node)
            validator_msgs_fount = len(list(filter(lambda x: x.name == "Валидатор", state["messages"])))
            response = self.settings.call_llm(llm, request, "validator")
            result = response["instructions"]

            if response["status"] == "SUCCESS" or validator_msgs_fount + 1 > 3:
                goto = END
            else:
                goto = return_node
            print(f"STATUS: {response['status']}\n\nVALIDATOR: {result}")
            return Command(
                update={
                    "messages": old_messages + [HumanMessage(content=result, name="Валидатор")],
                },
                goto=goto
            )
        return darch_validator_node
