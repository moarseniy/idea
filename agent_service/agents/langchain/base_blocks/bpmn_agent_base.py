from abstract.singleton import Singleton
from ..agent_states import BpmnAgentState
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
import os, time

from ..bpmn_utils import validate_bpmn, save_xml_file

class BpmnAgentBuilder(Singleton):
    def _setup(self):
        self.settings = AgentSettings()

    def create_bpmn_agent(self):
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

    def create_describtion_agent(self):
        tools = []
        memory_describer = ConversationBufferMemory(memory_key="chat_history")
        llm = self.settings.select_model()

        describtion_agent = initialize_agent(
            tools=tools,
            llm=llm,
            memory=memory_describer,
            agent=AgentType.CONVERSATIONAL_REACT_DESCRIPTION,
            verbose=True,
            handle_parsing_errors=True
        )
        return describtion_agent

    def create_bpmn_description_node(self, describer_agent):
        def describtion_agent_node(state: BpmnAgentState) -> Command[Literal["bpmn_agent"]]:
            describer_prompt = self.settings.bpmn_describer_template
            request = describer_prompt.format(task=state["task"])
            
            print(f"AAAAAAAAAAAAAAAAREQUEST: {request}")
            start_time = time.time()
            
            response = describer_agent.run(request)
            
            end_time = time.time() - start_time 
            print(f"AAAAAAAAAAAAAAAATIME: {end_time:.2f} seconds")

            if isinstance(response, dict):
                result = response["output"]
            else:
                result = response
            return Command(
                update={
                    "description": result
                },
                goto="bpmn_agent"
            )
        return describtion_agent_node

    def create_bpmn_agent_node(self, bpmn_agent):
        def bpmn_agent_node(state: BpmnAgentState) -> Command[Literal["bpmn_validator"]]:
            bpmn_agent_prompt = self.settings.bpmn_template
            bpmn_instruction = self.settings.bpmn_instruction

            if "messages" in state and state["messages"]:
                request = state["messages"][-1].content
                old_messages = state["messages"]
            else:
                old_messages = []
                request = bpmn_agent_prompt.format(bpmn_instruction=bpmn_instruction, description=state["description"])
            print(f"XXXXXXXXX{request}")
            response = bpmn_agent.run(request)
            print(f"XXXXXXXXX{response}")
            if isinstance(response, dict):
                result = response["output"]
            else:
                result = response
            return Command(
                update={
                    "messages": old_messages + [HumanMessage(content=result, name="Аналитик")],
                    "bpmn": result
                },
                goto="bpmn_validator"
            )

        return bpmn_agent_node

    def create_bpmn_validator_node(self):
        llm = self.settings.select_model()
        
        def bpmn_validator_node(state: BpmnAgentState) -> Command[Literal["bpmn_agent", END]]:
            return_node = "bpmn_agent"
            bpmn_validator_prompt = self.settings.bpmn_validator_prompt
            if "messages" in state and state["messages"]:
                
                print("BBBBBBBBBBBB", state["bpmn"])
                bpmn_schema = state["bpmn"].split('```')[1]
                file_path = save_xml_file(bpmn_schema)
                validation_info = validate_bpmn(file_path)

                validation_info.replace("error  Import warning: unparsable content xml", "") # хак чтобы убрать ошибку со словом xml в начале
                error_count = validation_info.lower().split().count("error")

                if error_count < 2:
                    validation_info = "Ошибок нет." # dirty hack (слово errors не учитывается)

                print("CCCCCCCCCCC", validation_info)

                # request += "\n Помимо этого, диаграмма возможно содержит следующие ошибки:{validation_info}, на которые нужно обратить внимание."
                # print(f"VALIDATOR_REQUEST:{request}")
                
                prompt = bpmn_validator_prompt.format(description=state["description"], validation_info=validation_info)
                
                print(f"VALIDATOR_PROMPT: {prompt}")

                request = [SystemMessage(content=prompt)] + state["messages"]
                old_messages = state["messages"]
            else:
                return Command(goto=return_node)


            validator_msgs_fount = len(list(filter(lambda x: x.name == "Валидатор", state["messages"])))
            response = self.settings.call_llm(llm, request, "validator")
            result = response["instructions"]
            print(f"VALIDATOR_RESPONSE:{response}")
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
