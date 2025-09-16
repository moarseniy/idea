from .builder import LangChainBuilder
from .agent_responses import JsonValidatorResponse
from settings import AgentSettings

import re

from langchain.schema import HumanMessage, AIMessage, SystemMessage
from langchain.memory import ConversationBufferMemory, ChatMessageHistory

class LangchainApp:
    def __init__(self, ba_requirements=False, sa_requirements=False, bpmn_schema=False, json_schema=True, memory_json=None):
        builder = LangChainBuilder()
        self.settings = AgentSettings()
        self.ba_agent_chain = builder.create_ba_chain()
        self.sa_agent_chain = builder.create_sa_chain()
        self.bpmn_agent_chain = builder.create_bpmn_chain()
        memory_json = self.memory_from_list(memory_json)
        self._json_analitic_agent = builder.create_json_analitic_agent(memory=memory_json)
        self._json_corrector_agent = builder.create_json_corrector_agent()

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

    async def ba_agent(self, initial_state):
        result = await self.ba_agent_chain.ainvoke(initial_state)
        return "businessRequirements", result["result"]

    async def sa_agent(self, initial_state):
        result = await self.sa_agent_chain.ainvoke(initial_state)
        return "systemRequirements", result["sa_requirements"]

    async def bpmn_agent(self, initial_state):
        result = await self.bpmn_agent_chain.ainvoke(initial_state)
        answer = dict()
        result = result["bpmn"]
        match_xml = re.search(r'```xml\s*(.*?)\s*```', result, re.DOTALL)
        if match_xml:
            answer["xml"] = match_xml.group(1)
        else:
            answer["xml"] = result
        return "bmpnSchema", answer["xml"]

    async def json_analitic_agent(self, task):
        if not self._json_analitic_agent.memory.chat_memory.messages:
            json_instructions = self.settings.json_instruction
            request = self.settings.json_template.format(task=task, definitions=json_instructions)
        else:
            request = task
        response = await self._json_analitic_agent.ainvoke(request)
        if isinstance(response, dict):
            result = response["output"]
        else:
            result = response
        answer = dict()
        match_json = re.search(r'```json\s*(.*?)\s*```', result, re.DOTALL)
        if match_json:
            answer["json"] = match_json.group(1)

        matches = re.findall(r'\[ВОПРОС\](.*?)\[КОНЕЦ ВОПРОСА\]', result, re.DOTALL)
        if matches:
            if len(matches)==1:
                answer["question"] = matches[0]
            else:
                answer["question"] = "\n".join([f"{i+1}. {ques}" for i, ques in matches])
            answer["history"] = self.memory_to_list(self._json_analitic_agent.memory)

        if not answer:
            answer["json"] = result
        return answer

    async def json_corrector_agent(self, history, task):
        system = self.settings.json_corrector_system_prompt
        user = self.settings.json_corrector_user_prompt.format(task=task, json=history["jsonSchema"])
        request = [
            SystemMessage(content=system),
            HumanMessage(content=user, name="Пользователь")
        ]
        result = await self._json_corrector_agent.with_structured_output(JsonValidatorResponse).ainvoke(request)
        if result["status"] == "FAIL":
            result["result"] = "Указанная интеграция отсутствует."
        else:
            match_json = re.search(r'```json\s*(.*?)\s*```', result["result"], re.DOTALL)
            if match_json:
                result["result"] = match_json.group(1)
        print(result)
        return result

    async def ba_corrector_agent(self, history, task):
        system = self.settings.ba_corrector_system_prompt
        user = self.settings.ba_corrector_user_prompt.format(task=task, ba_requirements=history["businessRequirements"])
        request = [
            SystemMessage(content=system),
            HumanMessage(content=user, name="Пользователь")
        ]
        result = await self._json_corrector_agent.ainvoke(request)
        print(f"BA_CORRECTOR_RESULT: {result}")
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

    async def bpmn_corrector_agent(self, history, task):
        system = self.settings.bpmn_corrector_system_prompt
        user = self.settings.bpmn_corrector_user_prompt.format(task=task, bpmn=history["bpmnSchema"])
        request = [
            SystemMessage(content=system),
            HumanMessage(content=user, name="Пользователь")
        ]
        result = await self._json_corrector_agent.ainvoke(request)
        result = result.content
        match_xml = re.search(r'```xml\s*(.*?)\s*```', result, re.DOTALL)
        if match_xml:
            result = match_xml.group(1)
        print(f"BPMN_CORRECTOR_RESULT: {result}")
        return result


