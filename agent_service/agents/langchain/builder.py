from abstract.singleton import Singleton
from .agent_states import BaAgentState, SaAgentState, BpmnAgentState
from .agent_responses import ValidatorResponse
from .base_blocks.ba_agent_base import BaAgentBuilder
from .base_blocks.bpmn_agent_base import BpmnAgentBuilder
from .base_blocks.sa_agent_base import SaAgentBuilder
from .base_blocks.json_agent_base import JsonAgentBuilder
from .base_blocks.corrector_agent_base import CorrectionAgentBuilder
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


class LangChainBuilder(Singleton):
    def _setup(self):
        self.settings = AgentSettings()
        self.ba_builder = BaAgentBuilder()
        self.sa_builder = SaAgentBuilder()
        self.bpmn_builder = BpmnAgentBuilder()
        self.corrector_builder = CorrectionAgentBuilder()
        self.json_builder = JsonAgentBuilder()
        pass

    def make_graph(self, mapper, entry_point, state_schema):
        builder = StateGraph(state_schema=state_schema)
        builder.set_entry_point(entry_point)
        for name, node in mapper.items():
            builder.add_node(name, node)
        graph = builder.compile()
        return graph

    def create_ba_chain(self):
        ba_agent = self.ba_builder.create_ba_agent()
        ba_agent_node = self.ba_builder.create_ba_agent_node(ba_agent)
        ba_validator_node = self.ba_builder.create_ba_validator_node()

        graph_mapper = {
            "ba_agent": ba_agent_node,
            "ba_validator": ba_validator_node
        }

        graph = self.make_graph(graph_mapper, "ba_agent", BaAgentState)
        return graph

    def create_sa_chain(self):
        analitic_agent = self.sa_builder.create_sa_analitic_agent()
        generator_agent = self.sa_builder.create_sa_generator_agent()
        analitic_node = self.sa_builder.create_sa_analitic_node(analitic_agent)
        generator_node = self.sa_builder.create_sa_generator_node(generator_agent)
        validator_node = self.sa_builder.create_sa_validator_node()

        graph_mapper = {
            "analitic_agent": analitic_node,
            "generator_agent": generator_node,
            "validator_agent": validator_node
        }
        graph = self.make_graph(graph_mapper, "analitic_agent", SaAgentState)
        return graph

    def create_bpmn_chain(self):
        describer_agent = self.bpmn_builder.create_describtion_agent()
        bpmn_agent = self.bpmn_builder.create_bpmn_agent()
        bpmn_describer_agent_node = self.bpmn_builder.create_bpmn_description_node(describer_agent)
        bpmn_agent_node = self.bpmn_builder.create_bpmn_agent_node(bpmn_agent)
        bpmn_validator_node = self.bpmn_builder.create_bpmn_validator_node()

        graph_mapper = {
            "describer": bpmn_describer_agent_node,
            "bpmn_agent": bpmn_agent_node,
            "bpmn_validator": bpmn_validator_node
        }
        graph = self.make_graph(mapper=graph_mapper, entry_point="describer", state_schema=BpmnAgentState)
        return graph

    def create_json_analitic_agent(self, memory=None):
        analitic_agent = self.json_builder.create_json_analitic_agent(memory=memory)
        return analitic_agent

    def create_json_corrector_agent(self):
        corrector_agent = self.json_builder.create_json_corrector_agent()
        return corrector_agent



