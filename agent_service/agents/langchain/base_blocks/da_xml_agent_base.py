from abstract.singleton import Singleton
from ..agent_states import DaXmlAgentState
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
import os, json, re, ast

from scripts.entities_patch_validator import validate_patch

def extract_json_data(text: str):
    pattern = r"```json\s*(.*?)```"
    matches = re.findall(pattern, text, re.DOTALL | re.IGNORECASE)
    return "\n\n".join(m.strip() for m in matches)#[m.strip() for m in matches]

from langchain.agents import AgentOutputParser
from langchain.schema import AgentFinish
from typing import Any, Dict, Union

class NoOpAgentOutputParser(AgentOutputParser):
    """Простой парсер, который никогда не ломается и всегда возвращает сырой текст."""

    def parse(self, text: str) -> AgentFinish:
        # возвращаем всё как есть в виде "Final Answer"
        return AgentFinish(
            return_values={"output": text},
            log=text
        )

    @property
    def _type(self) -> str:
        return "no_op"

class DaXmlAgentBuilder(Singleton):
    def _setup(self):
        self.settings = AgentSettings()

    def create_da_xml_agent(self):
        llm = self.settings.select_model()

        memory_da_xml = ConversationBufferMemory(memory_key="chat_history")
        tools = []

        agent_output_parser = NoOpAgentOutputParser()
        
        da_xml_agent = initialize_agent(
            tools=tools,
            llm=llm,
            memory=memory_da_xml,
            agent=AgentType.CONVERSATIONAL_REACT_DESCRIPTION,#CHAT_ZERO_SHOT_REACT_DESCRIPTION,
            return_intermediate_steps=False,
            verbose=True,
            handle_parsing_errors=True,
            agent_kwargs={"output_parser": agent_output_parser}
        )
        return da_xml_agent

    def create_da_xml_agent_node(self, da_xml_agent):
        def da_xml_agent_node(state: DaXmlAgentState) -> Command[Literal["da_xml_validator"]]:
            da_xml_prompt = self.settings.da_xml_template
            da_xml_instruction = self.settings.da_xml_instruction

            if "messages" in state and state["messages"]:
                old_messages = state["messages"]
                request = state["messages"][-1].content
            else:
                old_messages = []
                print("(create_da_xml_agent_node)", state["task"])
                request = da_xml_prompt.format(task=state["task"])
                print("(create_da_xml_agent_node)", request)

            response = da_xml_agent.run(request)
            if isinstance(response, dict):
                result = response["output"]
            else:
                result = response#.replace('json','').replace('````','')
            print("(create_da_xml_agent_node111)", result)
            
            return Command(
                update={
                    "result": result,
                    "messages": old_messages + [HumanMessage(content=result, name="Аналитик")]
                },
                goto="da_xml_validator"
            )

        return da_xml_agent_node

    def create_da_xml_validator_node(self):
        llm_max = self.settings.select_model()
        def da_xml_validator_node(state: DaXmlAgentState) -> Command[Literal["da_xml_agent", END]]:
            da_xml_validator_prompt = self.settings.da_xml_validator_prompt
            return_node = "da_xml_agent"
            if not "messages" in state or not state["messages"]:
                return Command(goto=return_node)

            prompt = da_xml_validator_prompt.format(task=state["task"])
            system = SystemMessage(content=prompt)
            request = [system] + state["messages"]

            print(f"VALIDATOR_REQUEST: {request}")

            validator_msgs_fount = len(list(filter(lambda x: x.name == "Валидатор", state["messages"])))
            
            # response = self.settings.call_llm(llm_max, request, "validator")
            
            # result = response["instructions"]

            # print("state[\"task\"]: ", state["task"])

            # print("state[\"messages\"]: ", state["messages"])

            print("state[\"result\"]: ", state["result"])

            # print("PROFILE_JSON111: ", state["profile"])

            task_json = state["result"].replace("Отчет:", "")
            if "`" in task_json and "json" in task_json:
                task_json = extract_json_data(task_json)
            
            print("TASK_JSON", task_json)
            
            if not isinstance(task_json, dict):
                task_json = ast.literal_eval(task_json)

            profile_json = state["task"]
            if not isinstance(profile_json, dict):
                profile_json = ast.literal_eval(profile_json)

            print("PROFILE_JSON", profile_json)

            json_status = validate_patch(profile_json, task_json)

            # print(f"STATUS: {response['status']}\n\nXML ANALYTIC VALIDATOR: {result}")
            print(f"PATCH_VALIDATOR_STATUS: {json_status}")

            result = "Всё отлично, отчет хороший!"
            if json_status == "SUCCESS" or validator_msgs_fount + 1 > 3:
                goto = END
            else:
                result = "\nЕсть ряд важных ошибок, которые нужно исправить: \n" + json_status
                goto = return_node

            return Command(
                update={
                    "messages": state["messages"] + [HumanMessage(content=result, name="Валидатор")],
                },
                goto=goto
            )
        return da_xml_validator_node
