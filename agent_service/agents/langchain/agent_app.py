from .builder import LangChainBuilder
from .agent_responses import JsonValidatorResponse
from settings import AgentSettings

import re

from langchain.schema import HumanMessage, AIMessage, SystemMessage
from langchain.memory import ConversationBufferMemory, ChatMessageHistory

class LangchainApp:
    def __init__(self, de_requirements=False, darch_requirements=False, memory_json=None):
        builder = LangChainBuilder()
        self.settings = AgentSettings()
        self.de_agent_chain = builder.create_de_chain()
        self.darch_agent_chain = builder.create_darch_chain()
        memory_json = self.memory_from_list(memory_json)
        
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

    async def de_agent(self, initial_state):
        result = await self.de_agent_chain.ainvoke(initial_state)
        return "engineerRequirements", result["result"]

    async def darch_agent(self, initial_state):
        result = await self.darch_agent_chain.ainvoke(initial_state)
        return "darchRequirements", result["darch_requirements"]

    async def de_corrector_agent(self, history, task):
        system = self.settings.de_corrector_system_prompt
        user = self.settings.de_corrector_user_prompt.format(task=task, de_requirements=history["engineerRequirements"])
        request = [
            SystemMessage(content=system),
            HumanMessage(content=user, name="Пользователь")
        ]
        result = await self._json_corrector_agent.ainvoke(request)
        print(f"DE_CORRECTOR_RESULT: {result}")
        return result.content

    async def sa_corrector_agent(self, history, task):
        system = self.settings.sa_corrector_system_prompt
        user = self.settings.sa_corrector_user_prompt.format(task=task, sa_requirements=history["systemRequirements"])
        request = [
            SystemMessage(content=system),
            HumanMessage(content=user, name="Пользователь")
        ]
        result = await self._json_corrector_agent.ainvoke(request)
        print(f"SA_CORRECTOR_RESULT: {result}")
        return result.content

