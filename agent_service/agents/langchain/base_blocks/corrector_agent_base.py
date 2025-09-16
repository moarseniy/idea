from abstract.singleton import Singleton
from ..agent_states import CorrectorAgentState
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

class CorrectionAgentBuilder(Singleton):
    def _setup(self):
        self.settings = AgentSettings()

    def create_ba_corrector_agent(self):
        llm = self.settings.select_model()

        memory_ba = ConversationBufferMemory(memory_key="chat_history")
        tools = []
        ba_agent = initialize_agent(
            tools=tools,
            llm=llm,
            memory=memory_ba,
            agent=AgentType.CONVERSATIONAL_REACT_DESCRIPTION,
            verbose=True,
            handle_parsing_errors=True
        )
        return ba_agent

    def create_sa_corrector_agent(self):
        llm = self.settings.select_model()

        memory_sa = ConversationBufferMemory(memory_key="chat_history")
        tools = []
        sa_agent = initialize_agent(
            tools=tools,
            llm=llm,
            memory=memory_sa,
            agent=AgentType.CONVERSATIONAL_REACT_DESCRIPTION,
            verbose=True,
            handle_parsing_errors=True
        )
        return sa_agent

    def create_bpmn_corrector_agent(self):
        llm = self.settings.select_model()

        memory_bpmn = ConversationBufferMemory(memory_key="chat_history")
        tools = []
        bpmn_agent = initialize_agent(
            tools=tools,
            llm=llm,
            memory=memory_bpmn,
            agent=AgentType.CONVERSATIONAL_REACT_DESCRIPTION,
            verbose=True,
            handle_parsing_errors=True
        )
        return bpmn_agent

    def create_json_corrector_agent(self):
        llm = self.settings.select_model()

        memory_json = ConversationBufferMemory(memory_key="chat_history")
        tools = []
        json_agent = initialize_agent(
            tools=tools,
            llm=llm,
            memory=memory_json,
            agent=AgentType.CONVERSATIONAL_REACT_DESCRIPTION,
            verbose=True,
            handle_parsing_errors=True
        )
        return json_agent

    def create_correction_orchestrator_node(self):
        llm = self.settings.select_model()

        def orchestrator_node(state: CorrectorAgentState) -> Command[Literal["ba_agent", "bpmn_agent", "sa_agent", "json_agent"]]:
            orchestrator_prompt = self.settings.correction_orchestrator_prompt
            request = [SystemMessage(content=orchestrator_prompt)] + state["messages"]
            response = self.settings.call_llm(llm, request, "orchestrator")
            goto = response["next"]
            return Command(goto=goto)
        return orchestrator_node

    def create_correction_ba_node(self, ba_corrector_agent):
        def ba_requirements_node(state: CorrectorAgentState) -> Command[Literal["ba_validator"]]:
            task = state["task"]
            prev_context = state["prev_context"]
            ba_correction_prompt = self.settings.correction_ba_agent_prompt.format(task=task, prev_context=prev_context)
            if "messages" in state and state["messages"]:
                old_messages = state["messages"]
                request = state["messages"][-1].content
            else:
                old_messages = []
                request = ba_correction_prompt
            response = ba_corrector_agent.run(request)
            if isinstance(response, dict):
                result = response["output"]
            else:
                result = response
            goto = "ba_validator"
            return Command(
                update={
                    "ba_requirements": result,
                    "messages": old_messages + [HumanMessage(content=result, name="Аналитик")]
                },
                goto=goto
            )
        return ba_requirements_node

    def create_correction_ba_validator_node(self):
        llm = self.settings.select_model()

        def ba_validator_node(state: CorrectorAgentState) -> Command[Literal["ba_agent", END]]:
            return_node = "ba_agent"
            task = state["task"]
            prev_context = state["prev_context"]
            ba_validator_prompt = self.settings.correction_ba_validator_agent_prompt.format(task=task, prev_context=prev_context)
            if "messages" in state and state["messages"]:
                system_prompt = ba_validator_prompt
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
        return ba_validator_node

    def create_correction_sa_node(self, sa_corrector_agent):
        def sa_requirements_node(state: CorrectorAgentState) -> Command[Literal["sa_validator"]]:
            task = state["task"]
            prev_context = state["prev_context"]
            sa_correction_prompt = self.settings.correction_sa_agent_prompt.format(task=task, prev_context=prev_context)
            if "messages" in state and state["messages"]:
                old_messages = state["messages"]
                request = state["messages"][-1].content
            else:
                old_messages = []
                request = sa_correction_prompt
            response = sa_corrector_agent.run(request)
            if isinstance(response, dict):
                result = response["output"]
            else:
                result = response
            goto = "sa_validator"
            return Command(
                update={
                    "sa_requirements": result,
                    "messages": old_messages + [HumanMessage(content=result, name="Аналитик")]
                },
                goto=goto
            )
        return sa_requirements_node

    def create_correction_sa_validator_node(self):
        llm = self.settings.select_model()

        def sa_validator_node(state: CorrectorAgentState) -> Command[Literal["sa_agent", END]]:
            return_node = "sa_agent"
            task = state["task"]
            prev_context = state["prev_context"]
            sa_validator_prompt = self.settings.correction_sa_validator_agent_prompt.format(task=task, prev_context=prev_context)
            if "messages" in state and state["messages"]:
                system_prompt = sa_validator_prompt
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
        return sa_validator_node

    def create_correction_bpmn_node(self, bpmn_corrector_agent):
        def bpmn_node(state: CorrectorAgentState) -> Command[Literal["bpmn_validator"]]:
            task = state["task"]
            prev_context = state["prev_context"]
            bpmn_correction_prompt = self.settings.correction_bpmn_agent_prompt.format(task=task, prev_context=prev_context)
            if "messages" in state and state["messages"]:
                old_messages = state["messages"]
                request = state["messages"][-1].content
            else:
                old_messages = []
                request = bpmn_correction_prompt
            response = bpmn_corrector_agent.run(request)
            if isinstance(response, dict):
                result = response["output"]
            else:
                result = response
            goto = "bpmn_validator"
            return Command(
                update={
                    "bpmn": result,
                    "messages": old_messages + [HumanMessage(content=result, name="Аналитик")]
                },
                goto=goto
            )
        return bpmn_node

    def create_correction_bpmn_validator_node(self):
        llm = self.settings.select_model()
        
        def bpmn_validator_node(state: CorrectorAgentState) -> Command[Literal["bpmn_agent", END]]:
            return_node = "bpmn_agent"
            task = state["task"]
            prev_context = state["prev_context"]
            bpmn_validator_prompt = self.settings.correction_bpmn_validator_agent_prompt.format(task=task, prev_context=prev_context)
            if "messages" in state and state["messages"]:
                system_prompt = bpmn_validator_prompt
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
        return bpmn_validator_node