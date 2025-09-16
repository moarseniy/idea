from abstract.singleton import Singleton
from ..agent_states import BaAgentState
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

class BaAgentBuilder(Singleton):
    def _setup(self):
        self.settings = AgentSettings()

    def create_ba_agent(self):
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

    def create_ba_agent_node(self, ba_agent):
        def ba_agent_node(state: BaAgentState) -> Command[Literal["ba_validator"]]:
            ba_prompt = self.settings.ba_template
            ba_instruction = self.settings.ba_instruction

            if "messages" in state and state["messages"]:
                old_messages = state["messages"]
                request = state["messages"][-1].content
            else:
                old_messages = []
                request = ba_prompt.format(task=state["task"], ba_instruction=ba_instruction)

            response = ba_agent.run(request)
            if isinstance(response, dict):
                result = response["output"]
            else:
                result = response
            return Command(
                update={
                    "result": result,
                    "messages": old_messages + [HumanMessage(content=result, name="Аналитик")]
                },
                goto="ba_validator"
            )
        return ba_agent_node

    def create_ba_validator_node(self):
        llm_max = self.settings.select_model()
        def ba_validator_node(state: BaAgentState) -> Command[Literal["ba_agent", END]]:
            ba_validator_prompt = self.settings.ba_validator_prompt
            return_node = "ba_agent"
            if not "messages" in state or not state["messages"]:
                return Command(goto=return_node)

            prompt = ba_validator_prompt.format(task=state["task"])
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
        return ba_validator_node