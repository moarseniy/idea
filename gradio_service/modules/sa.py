import prompts

from langchain.agents import initialize_agent, AgentType
from langchain.memory import ConversationBufferMemory
from langchain_openai import ChatOpenAI as CH
from langchain.schema import HumanMessage, SystemMessage
from langchain.chains import RetrievalQA
from langgraph.types import Command

from langgraph.graph import StateGraph, END
from typing import Literal, List
from typing_extensions import TypedDict

import warnings
import os, json

warnings.filterwarnings("ignore")
os.environ["OPENAI_API_KEY"]="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"

def read_doc(file):
    with open(file, "r") as f:
        return f.read()

def log(s):
    with open("sa_log_2.txt", "a") as f:
        f.write("="*100 + "\n")
        f.write(s+"\n")

class AgentState(TypedDict):
    messages: List
    task: str
    elements: str
    ba_requirements: str
    sa_requirements: str
    recommendations: str
    result: str

class ValidatorResponse(TypedDict):
    ba_requirements: str
    bpmn_schema: str

def analitic_node(state:AgentState)->Command[Literal["generator_agent"]]:
    """Агент-аналитик составляет список элементов системы."""
    memory_analitic.clear()
    memory_.clear()
    query = prompts.analitic_system_prompt + "\n" * 2 + prompts.analitic_user_prompt.format(task=state["task"], ba_requirements=state["ba_requirements"])
    result = analitic_agent.run(query)
    # state["elements"] = result
    print(f"GOTO: generator_agent")
    return Command(
        update={
            "elements": result
        },
        goto="generator_agent"
    )

def generator_node(state:AgentState)->Command[Literal["validator_agent"]]:
    """Агент-генератор составляет системные требования, основываясь на бизнес-требованиях и элементах системы."""
    ba_requirements=state["ba_requirements"]
    elements=state["elements"]
    recommendations = state.get("recommendations", "")
    sa_requirements = state.get("sa_requirements", "")
    if not recommendations:
        _generator_system_prompt = prompts.generator_system_prompt
        _generator_user_prompt = prompts.generator_user_prompt.format(
            ba_requirements=ba_requirements, 
            elements=elements, 
        )
        
    else:
        _generator_system_prompt = prompts.generator_correct_system_prompt
        _generator_user_prompt = prompts.generator_correct_user_prompt.format(
            sa_requirements=sa_requirements,
            recommendations=recommendations
        )
        log(f"CORRECT: {recommendations}")
    
    message = [
        SystemMessage(content=_generator_system_prompt),
        HumanMessage(content=_generator_user_prompt)
    ]
    
    result = llm.invoke(message)
    print(f"GENERATOR:{result.content}")
    print(f"GOTO: validator_agent")
    return Command(
        update={
            "sa_requirements": result
        },
        goto="validator_agent"
    )

def validator_node(state:AgentState)->Command[Literal["generator_agent", END]]:
    """Агент-генератор составляет системные требования, основываясь на бизнес-требованиях и элементах системы."""
    sa_requirements=state["sa_requirements"]
    # query = validator_system_prompt + "\n"*2 + validator_user_prompt.format(sa_requirements=sa_requirements)
    # result = validator_agent.run(query)
    message = [
        SystemMessage(content=prompts.validator_system_prompt),
        HumanMessage(content=prompts.validator_user_prompt.format(sa_requirements=sa_requirements))
    ]
    result = llm.invoke(message)
    print(f"VALIDATOR: {result.content}")
    if "success" in result.content.lower():
        print(f"GOTO: {END}")
        return Command(
            update={
                "result": result
            },
            goto = END
        )
    else:
        print(f"GOTO: generator_agent")
        return Command(
            update={
            "recommendations": result
        },
        goto="generator_agent"
    )

if __name__ == '__main__':
	sa_instruction = read_doc("../instructions/sa_instruction.md")

	llm = CH(model="gpt-4o-mini", temperature=0.03)

	memory_analitic = ConversationBufferMemory(memory_key="chat_history")
	memory_ = ConversationBufferMemory(memory_key="chat_history")
	memory_ = ConversationBufferMemory(memory_key="chat_history")
	tools = []

	memory_analitic.clear()

	analitic_agent = initialize_agent(
	    tools=tools,
	    llm=llm,
	    memory = memory_analitic,
	    agent=AgentType.CONVERSATIONAL_REACT_DESCRIPTION,
	    verbose=True,
	    handle_parsing_errors=True
	)

	generator_agent = initialize_agent(
	    tools=tools,
	    llm=llm,
	    agent=AgentType.CONVERSATIONAL_REACT_DESCRIPTION,
	    memory = memory_,
	    verbose=True,
	    handle_parsing_errors=True
	)

	validator_agent = initialize_agent(
	    tools=tools,
	    llm=llm,
	    agent=AgentType.CONVERSATIONAL_REACT_DESCRIPTION,
	    memory = memory_,
	    verbose=True,
	    handle_parsing_errors=True
	)

	builder = StateGraph(state_schema=AgentState)

	builder.set_entry_point("analitic_agent")
	builder.add_node("analitic_agent", analitic_node)
	builder.add_node("generator_agent", generator_node)
	builder.add_node("validator_agent", validator_node)

	graph = builder.compile()

	with open("../instructions/gpt_4_max.json", "r") as f:
	    prev_result = json.load(f)
	ba_requirements = prev_result["ba_requirements"]

	task = """Разработать сервис, который поможет аналитикам формировать диаграммы процессов.
	- Аналитик описывает процесс голосом;
	- Система генерирует диаграмму и отображает ее аналитику;
	- Система в режиме чата с аналитиком вносит правки в диаграмму.
	- Подразумеватся web-приложение.
	"""

	initial_state = {
	    "task": task,
	    "ba_requirements": ba_requirements
	}

	sa_graph_result = graph.invoke(initial_state)