from abstract.singleton import Singleton
from ..agent_states import DaAgentState
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

class DaAgentBuilder(Singleton):
    def _setup(self):
        self.settings = AgentSettings()

    def create_da_agent(self):
        llm = self.settings.select_model()

        memory_da = ConversationBufferMemory(memory_key="chat_history")
        tools = []
        da_agent = initialize_agent(
            tools=tools,
            llm=llm,
            memory=memory_da,
            agent=AgentType.CONVERSATIONAL_REACT_DESCRIPTION,
            verbose=True,
            handle_parsing_errors=True
        )
        return da_agent

    def create_da_agent_node(self, da_agent):
        def da_agent_node(state: DaAgentState) -> Command[Literal["da_validator"]]:
            da_prompt = self.settings.da_template
            da_instruction = self.settings.da_instruction

            if "messages" in state and state["messages"]:
                old_messages = state["messages"]
                request = state["messages"][-1].content
            else:
                old_messages = []
                print("(create_da_agent_node)", state["task"])
                request = da_prompt.format(task=state["task"])
                print("(create_da_agent_node)", request)

            response = da_agent.run(request)
            if isinstance(response, dict):
                result = response["output"]
            else:
                result = response#.replace('json','').replace('````','')
            print("(create_da_agent_node111)", result)
            return Command(
                update={
                    "result": result,
                    "messages": old_messages + [HumanMessage(content=result, name="Аналитик")]
                },
                goto="da_validator"
            )
        return da_agent_node

    def create_da_validator_node(self):
        llm_max = self.settings.select_model()
        def da_validator_node(state: DaAgentState) -> Command[Literal["da_agent", END]]:
            da_validator_prompt = self.settings.da_validator_prompt
            return_node = "da_agent"
            if not "messages" in state or not state["messages"]:
                return Command(goto=return_node)

            prompt = da_validator_prompt.format(task=state["task"])
            system = SystemMessage(content=prompt)
            request = [system] + state["messages"]

            validator_msgs_fount = len(list(filter(lambda x: x.name == "Валидатор", state["messages"])))
            response = self.settings.call_llm(llm_max, request, "validator")
            
            result = response["instructions"]
            print(f"STATUS: {response['status']}\n\nANALYTIC VALIDATOR: {result}")

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
        return da_validator_node
