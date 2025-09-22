from abstract.singleton import Singleton
from .agent_states import DeAgentState, DarchAgentState
from .agent_responses import ValidatorResponse
from .base_blocks.de_agent_base import DeAgentBuilder
from .base_blocks.darch_agent_base import DarchAgentBuilder
from .base_blocks.corrector_agent_base import CorrectionAgentBuilder
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


class LangChainBuilder(Singleton):
    def _setup(self):
        self.settings = AgentSettings()
        self.de_builder = DeAgentBuilder()
        self.darch_builder = DarchAgentBuilder()
        self.corrector_builder = CorrectionAgentBuilder()
        pass

    def make_graph(self, mapper, entry_point, state_schema):
        builder = StateGraph(state_schema=state_schema)
        builder.set_entry_point(entry_point)
        for name, node in mapper.items():
            builder.add_node(name, node)
        graph = builder.compile()
        return graph

    def create_ba_chain(self):
        de_agent = self.de_builder.create_de_agent()
        de_agent_node = self.de_builder.create_de_agent_node(de_agent)
        de_validator_node = self.de_builder.create_de_validator_node()
        
        de_corrector_agent = self.corrector_builder.create_de_corrector_agent()
        de_corrector_node = self.corrector_builder.create_correction_de_node(de_corrector_agent)
        de_corrector_validator_node = self.corrector_builder.create_correction_de_validator_node()

        graph_mapper = {
            "de_agent": de_agent_node,
            "de_validator": de_validator_node,
            "de_corrector": de_corrector_node,
            "de_corrector_validator": de_corrector_validator_node
        }

        graph = self.make_graph(graph_mapper, "de_agent", DeAgentState)
        return graph

    def create_darch_chain(self):
        darch_agent = self.darch_builder.create_darch_agent()
        darch_agent_node = self.darch_builder.create_darch_agent_node(darch_agent)
        darch_validator_node = self.darch_builder.create_darch_validator_node()

        darch_corrector_agent = self.corrector_builder.create_darch_corrector_agent()
        darch_corrector_node = self.corrector_builder.create_correction_darch_node(darch_corrector_agent)
        darch_corrector_validator_node = self.corrector_builder.create_correction_darch_validator_node()

        graph_mapper = {
            "darch_agent": darch_agent_node,
            "darch_validator": darch_validator_node,
            "darch_corrector": darch_corrector_node,
            "darch_corrector_validator": darch_corrector_validator_node
        }
        
        graph = self.make_graph(graph_mapper, "darch_agent", DarchAgentState)
        return graph


