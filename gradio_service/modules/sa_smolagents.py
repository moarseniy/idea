from smolagents import tool, ToolCallingAgent
from smolagents import OpenAIServerModel
import json

from typing import Literal, List
from typing_extensions import TypedDict, NotRequired

from langgraph.graph import StateGraph, END
from langgraph.types import Command

import prompts

oa_api = "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"

# model_id = "meta-llama/Llama-3.2-3B-Instruct"
# model = TransformersModel(model_id=model_id)

model = OpenAIServerModel(
    model_id="gpt-4o-mini",
    api_base="https://api.openai.com/v1",
    api_key=oa_api, #os.environ["OPENAI_API_KEY"],
)

# Установите ваш API ключ
# import os
# os.environ["OPENAI_API_KEY"] = "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"

# hf_token = "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"
# model = HfApiModel(token=hf_token)

class AgentState(TypedDict):
    messages: NotRequired[List]
    task: str
    elements: NotRequired[str]
    ba_requirements: str
    sa_requirements: NotRequired[str]
    recommendations: NotRequired[str]
    result: NotRequired[str]

class ValidatorResponse(TypedDict):
    ba_requirements: str
    bpmn_schema: str

def read_doc(file):
    with open(file, "r") as f:
        return f.read()

def log(s):
    with open("sa_log_2.txt", "a") as f:
        f.write("="*100 + "\n")
        f.write(s+"\n")

sa_instruction = read_doc("../instructions/sa_instruction.md")

def analytic_node(state:AgentState)->Command[Literal["generator_agent"]]:
    """Агент-аналитик составляет список элементов системы."""
    # memory_analytic.clear()
    # memory_.clear()

    query = prompts.analytic_system_prompt + "\n" * 2 + prompts.analytic_user_prompt.format(task=state["task"], ba_requirements=state["ba_requirements"])
    result = analytic_agent.run(query)
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
    
    # message = [
    #     SystemMessage(content=_generator_system_prompt),
    #     HumanMessage(content=_generator_user_prompt)
    # ]
    
    # result = llm.invoke(message)
    # print(f"GENERATOR:{result.content}")
    
    query = _generator_system_prompt + "\n"*2 + _generator_user_prompt
    result = generator_agent.run(query)
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
    # result = llm.invoke(message)
    # print(f"VALIDATOR: {result.content}")
    # if "success" in result.content.lower():

    query = prompts.validator_system_prompt + "\n" * 2 + prompts.validator_user_prompt.format(sa_requirements=sa_requirements)
    result = validator_agent.run(message)

    if "success" in result.lower():
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

@tool
def update_analytic_agent_prompt(state: AgentState) -> str:
    """Запускается перед вызовом соответствующего агента. Обновляет текущий промпт.

    Args:
        state (AgentState): Текущее состояние агента с полями:
            - task: Текст задачи
            - ba_requirements: Бизнес-требования
            - elements: Элементы системы
            - sa_requirements: Системные требования
            - recommendations: рекомендации
            - result: результат

    Returns:
        str: Обновленный промпт.
    """
    
    prompt = prompts.analytic_system_prompt + "\n\n" + prompts.analytic_user_prompt.format(
        task=state.task, 
        ba_requirements=state.ba_requirements
    )
    
    return query

@tool
def update_generator_agent_prompt(state: AgentState) -> str:
    """Запускается перед вызовом соответствующего агента. Обновляет текущий промпт.

    Args:
        state (AgentState): Текущее состояние агента с полями:
            - task: Текст задачи
            - ba_requirements: Бизнес-требования
            - elements: Элементы системы
            - sa_requirements: Системные требования
            - recommendations: рекомендации
            - result: результат

    Returns:
        str: Обновленный промпт.
    """
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
        _generator_system_prompt,
        _generator_user_prompt
    ]
    
    return " ".join(message)

@tool
def update_validator_agent_prompt(state: AgentState) -> str:
    """Запускается перед вызовом соответствующего агента. Обновляет текущий промпт.

    Args:
        state (AgentState): Текущее состояние агента с полями:
            - task: Текст задачи
            - ba_requirements: Бизнес-требования
            - elements: Элементы системы
            - sa_requirements: Системные требования
            - recommendations: рекомендации
            - result: результат

    Returns:
        str: Обновленный промпт.
    """

    sa_requirements=state["sa_requirements"]
    # query = validator_system_prompt + "\n"*2 + validator_user_prompt.format(sa_requirements=sa_requirements)
    # result = validator_agent.run(query)
    message = [
        prompts.validator_system_prompt,
        prompts.validator_user_prompt.format(sa_requirements=sa_requirements)
    ]
    
    return " ".join(message)

if __name__ == "__main__":

    with open("../instructions/gpt_4_max.json") as f:
        prev_result = json.load(f)
    ba_requirements = prev_result["ba_requirements"]

    task = """Разработать сервис, который поможет аналитикам формировать диаграммы процессов.
    - Аналитик описывает процесс голосом;
    - Система генерирует диаграмму и отображает ее аналитику;
    - Система в режиме чата с аналитиком вносит правки в диаграмму.
    - Подразумеватся web-приложение.
    """

    analytic_prompt = prompts.analytic_system_prompt + "\n\n" + prompts.analytic_user_prompt.format(
        task=task, 
        ba_requirements=ba_requirements
    )

    analytic_agent = ToolCallingAgent(
        tools=[], 
        model=model, 
        add_base_tools=False,
        name="analytic_agent",
        description=analytic_prompt)
    
    generator_agent = ToolCallingAgent(
        tools=[], 
        model=model, 
        add_base_tools=False,
        name="generator_agent",
        description="Агент, который занимается генерацией контента.")
    
    validator_agent = ToolCallingAgent(
        tools=[], 
        model=model, 
        add_base_tools=False,
        name="validator_agent",
        description="Агент, который занимается валидацией сгенерированного контента.")

    # manager_agent = CodeAgent(
    #     tools=[],
    #     model=model,
    #     managed_agents=[analytic_agent, generator_agent, validator_agent],
    #     additional_authorized_imports=[],
    #     name=manager_agent,
    #     description="Координирует работу агентов, вызывая при необходимости каждый. При получении SUCCESS от validator_agent прекращает работу и выводит полученный результат."
    # )
    
    # manager_agent.run(task)

    builder = StateGraph(state_schema=AgentState)

    builder.set_entry_point("analytic_agent")
    builder.add_node("analytic_agent", analytic_node)
    builder.add_node("generator_agent", generator_node)
    builder.add_node("validator_agent", validator_node)

    graph = builder.compile()

    initial_state : AgentState = {
        "task": task,
        "ba_requirements": ba_requirements
    }

    sa_graph_result = graph.invoke(initial_state)
    
    print("AAAAAAAAAAAAAAAA")
    print(sa_graph_result["sa_requirements"])
    