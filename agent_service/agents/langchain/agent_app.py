from .builder import LangChainBuilder
from settings import AgentSettings

import re

from langchain.schema import HumanMessage, AIMessage, SystemMessage
from langchain.memory import ConversationBufferMemory, ChatMessageHistory

class LangchainApp:
    def __init__(self, da_requirements=False, da_json_requirements=False, de_requirements=False, darch_requirements=False):
        builder = LangChainBuilder()
        self.settings = AgentSettings()
        
        self.da_agent_chain = builder.create_da_chain()
        self.da_json_agent_chain = builder.create_da_json_chain()
        self.de_agent_chain = builder.create_de_chain()
        self.darch_agent_chain = builder.create_darch_chain()
        
        self.de_corrector_agent_chain = builder.create_de_corrector_chain()
        self.darch_corrector_agent_chain = builder.create_darch_corrector_chain()

    def memory_to_list(self, memory):
        messages = memory.chat_memory.messages
        prepared_messages = []
        for msg in messages:
            msg_dict = msg.dict()
            prepared_messages.append({"role": msg_dict["type"], "content": msg_dict["content"]})
        return prepared_messages

    def memory_from_list(self, messages):
        if not messages:
            return ConversationBufferMemory(memory_key="chat_history")
        prepared_messages = [
            HumanMessage(content=msg["content"]) if msg["role"] == "human" else AIMessage(content=msg["content"])
            for msg in messages
        ]
        chat_memory = ChatMessageHistory(messages=prepared_messages)
        return ConversationBufferMemory(chat_memory=chat_memory, memory_key="chat_history")

    async def da_json_agent(self, initial_state):
        result = await self.da_json_agent_chain.ainvoke(initial_state)
        return "daJsonRequirements", result["result"]

    async def da_agent(self, initial_state):
        result = await self.da_agent_chain.ainvoke(initial_state)
        return "daRequirements", result["result"]

    async def de_agent(self, initial_state):
        result = await self.de_agent_chain.ainvoke(initial_state)
        return "deRequirements", result["result"]

    async def darch_agent(self, initial_state):
        result = await self.darch_agent_chain.ainvoke(initial_state)
        return "darchRequirements", result["result"]

    async def de_corrector_agent(self, history, task):
        prev_context = history.get("deRequirements", "")
        initial_state = {
            "task": task,
            "prev_context": prev_context,
            "messages": []
        }
        result = await self.de_corrector_agent_chain.ainvoke(initial_state)
        print(f"DE_CORRECTOR_RESULT(agent_app.py): {result}")
        return result.get("de_requirements", "")

    async def darch_corrector_agent(self, history, task):
        prev_context = history.get("darchRequirements", "")
        initial_state = {
            "task": task,
            "prev_context": prev_context,
            "messages": []
        }
        result = await self.darch_corrector_agent_chain.ainvoke(initial_state)
        print(f"DARCH_CORRECTOR_RESULT(agent_app.py): {result}")
        return result.get("darch_requirements", "")

