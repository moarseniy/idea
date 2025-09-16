from wsgiref.validate import validator

from abstract.singleton import Singleton
from ..agent_states import SaAgentState
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

class SaAgentBuilder(Singleton):
    def _setup(self):
        self.settings = AgentSettings()

    def create_sa_analitic_agent(self):
        memory_analitic = ConversationBufferMemory(memory_key="chat_history")
        tools = []
        llm = self.settings.select_model()

        analitic_agent = initialize_agent(
            tools=tools,
            llm=llm,
            memory=memory_analitic,
            agent=AgentType.CONVERSATIONAL_REACT_DESCRIPTION,
            verbose=True,
            handle_parsing_errors=True
        )
        return analitic_agent

    def create_sa_generator_agent(self):
        memory_generator = ConversationBufferMemory(memory_key="chat_history")
        tools = []
        llm = self.settings.select_model()

        generator_agent = initialize_agent(
            tools=tools,
            llm=llm,
            agent=AgentType.CONVERSATIONAL_REACT_DESCRIPTION,
            memory=memory_generator,
            verbose=True,
            handle_parsing_errors=True
        )
        return generator_agent

    def create_sa_analitic_node(self, analitic_agent):
        def analitic_node(state: SaAgentState) -> Command[Literal["generator_agent"]]:
            """Агент-аналитик составляет список элементов системы."""
            request = self.settings.sa_analitic_template.format(task=state["task"], ba_requirements=state["ba_requirements"])
            response = analitic_agent.run(request)
            if isinstance(response, dict):
                result = response["output"]
            else:
                result = response
            return Command(
                update={
                    "elements": result
                },
                goto="generator_agent"
            )
        return analitic_node

    def create_sa_generator_node(self, generator_agent):
        def generator_node(state: SaAgentState) -> Command[Literal["validator_agent"]]:
            """Агент-генератор составляет системные требования, основываясь на бизнес-требованиях и элементах системы."""
            sa_instruction = self.settings.sa_instruction
            ba_requirements = state["ba_requirements"]
            elements = state["elements"]
            request = self.settings.sa_generator_template.format(
                sa_instruction=sa_instruction,
                ba_requirements=ba_requirements,
                elements=elements
            )
            if "messages" in state and state["messages"]:
                request = state["messages"][-1].content
                old_messages = state["messages"]
            else:
                old_messages = []

            response = generator_agent.invoke(request)
            result = response["output"]
            goto = "validator_agent"
            messages = old_messages + [HumanMessage(content=result, name="Аналитик")]
            return Command(
                update={
                    "sa_requirements": result,
                    "messages": messages
                },
                goto=goto
            )
        return generator_node

    def create_sa_validator_node(self):
        llm = self.settings.select_model()
        
        def validator_node(state: SaAgentState) -> Command[Literal["generator_agent", END]]:
            """Агент-генератор составляет системные требования, основываясь на бизнес-требованиях и элементах системы."""
            sa_instruction = self.settings.sa_instruction

            ba_requirements = state["ba_requirements"]
            system_prompt = self.settings.sa_validator_prompt.format(ba_requirements=ba_requirements,
                                                              sa_instruction=sa_instruction)
            system_msg = SystemMessage(content=system_prompt)
            message = [system_msg] + state["messages"]
            response = self.settings.call_llm(llm, message, "validator")
            msg = response["instructions"]
            validator_msgs_fount = len(list(filter(lambda x: x.name == "Валидатор", state["messages"])))
            if response["status"] == "SUCCESS" or validator_msgs_fount + 1 > 3:
                goto = END
            else:
                goto = "generator_agent"
            print(f"VALIDATOR: {msg}\n\nSTATUS{response['status']}")
            return Command(
                update={
                    "messages": state["messages"] + [HumanMessage(content=msg, name="Валидатор")]
                },
                goto=goto
            )
        return validator_node