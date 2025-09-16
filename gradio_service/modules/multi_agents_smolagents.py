from smolagents import ToolCallingAgent
from smolagents import OpenAIServerModel

import json

from typing import Literal, List, Dict
from typing_extensions import TypedDict

from langgraph.graph import StateGraph, END
from langgraph.types import Command

import prompts

oa_api = "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"

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

def display_graph(graph):
    from IPython.display import Image, display

    try:
        display(Image(graph.get_graph().draw_mermaid_png()))
    except Exception:
        # You can put your exception handling code here
        pass

class AgentState(TypedDict):
    messages: List
    task: str
    requirements: str
    bpmn: str
    result: Dict

class CompilerResponse(TypedDict):
    ba_requirements: str
    bpmn_schema: str

class Router(TypedDict):
    """Исполнитель для следующей задачи. Если исполнитель не требуется ответь FINISH."""
    next: Literal["ba_requirements_agent", "bpmn_agent", "compiler_agent", "FINISH"]

def read_doc(file):
    with open(file, "r") as f:
        return f.read()

def log(s):
    with open("sa_log_2.txt", "a") as f:
        f.write("="*100 + "\n")
        f.write(s+"\n")

ba_instruction = read_doc("../instructions/ba_instruction.md")
sa_instruction = read_doc("../instructions/sa_instruction.md")
bpmn_instruction = read_doc("../instructions/bpmn_instruction.md")


def orchestrator_node(state:AgentState)->Command[Literal["ba_requirements_agent", "bpmn_agent", "compiler_agent", "__end__"]]:
    """Агент-оркестратор выбирает следующий шаг."""
    # task = state["task"]
    # messages = [
    #     SystemMessage(content=prompts.orchestrator_system_prompt),
    #     HumanMessage(content=prompts.orchestrator_user_prompt.format(task=task))
    # ] + state["messages"]
    query = content= prompts.orchestrator_system_prompt + 2 * "\n" + prompts.orchestrator_user_prompt.format(task=task) + "\n".join(state["messages"])
    

    log(f"ORKESTRATOR STATE: {state}")
    # log(prompt)

    # decision = llm_2.with_structured_output(Router).invoke(messages)
    result = orchestrator_agent.run(query)

    goto = result #decision["next"]
    if goto == "FINISH":
        goto = END
    print(f"GOTO: {goto}")
    return Command(goto=goto)
    
def requirements_node(state:AgentState)->Command[Literal["orchestrator"]]:
    log(f"REQUIREMENTS STATE: {state}")
    task = state["task"]
    query = prompts.ba_system_prompt.format(ba_instruction=ba_instruction) + "\n\n" + f"Задача:\n{task}"
    result = req_agent.run(query)
    
    return Command(
        update={
            "messages": [
                HumanMessage(content=result, name="ba_requirements_agent")
            ],
            "requirements": result
        },
        goto="orchestrator"
    )

def bpmn_node(state:AgentState)->Command[Literal["orchestrator"]]:
    log(f"BPMN STATE {state}")
    reqs = state["requirements"]
    query = prompts.bpmn_system_prompt.format(bpmn_instruction=bpmn_instruction) + "\n\n" + f"Задача:\n{task}" + "\n\n" + f"Требования:\n{reqs}"
    result = bpmn_agent.run(query)
    return Command(
        update={
            "messages": [
                HumanMessage(content=result, name="bpmn_agent")
            ],
            "bpmn": result
        },
        goto="orchestrator"
    )

def compiler_node(state:AgentState)->Command[Literal["orchestrator"]]:
    log(f"COMPILER STATE {state}")
    ba_requirements = state["requirements"]
    bpmn_schema = state["bpmn"]
    # query = [
    #     SystemMessage(content=prompts.compiler_system_prompt),
    #     HumanMessage(content=prompts.compiler_user_prompt.format(ba_requirements=ba_requirements, bpmn_schema=bpmn_schema))
    # ]                

    query = prompts.compiler_system_prompt + 2 * "\n" + prompts.compiler_user_prompt.format(ba_requirements=ba_requirements, bpmn_schema=bpmn_schema)
    result = compiler_agent.run(query)
    # result = llm_2.with_structured_output(CompilerResponse).invoke(query)
    
    return Command(
        update={
            "messages": [
                HumanMessage(content="Успешно", name="compiler_agent")
            ],
            "result": result
        },
        goto="orchestrator"
    )

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

    req_agent = ToolCallingAgent(
        tools=[], 
        model=model, 
        add_base_tools=True,
        name="req_agent",
        description="Агент, который занимается требованиями.")
    
    ba_requirements_agent = ToolCallingAgent(
        tools=[], 
        model=model, 
        add_base_tools=True,
        name="ba_requirements_agent",
        description="Агент, который занимается генерацией бизнес аналитикой.")

    bpmn_agent = ToolCallingAgent(
        tools=[], 
        model=model, 
        add_base_tools=True,
        name="bpmn_agent",
        description="Агент, который занимается генерацией bpmn диаграмм.")
    
    compiler_agent = ToolCallingAgent(
        tools=[], 
        model=model, 
        add_base_tools=True,
        name="compiler_agent",
        description="Агент, который занимается компиляцией.")

    # TODO: change it
    orchestrator_agent = ToolCallingAgent(
        tools=[], 
        model=model, 
        managed_agents=[],
        add_base_tools=True,
        name="orchestrator_agent",
        description="Координирует работу агентов, вызывая при необходимости каждый. При получении SUCCESS от validator_agent прекращает работу и выводит полученный результат.")

    builder = StateGraph(state_schema=AgentState)

    builder.set_entry_point("orchestrator")
    builder.add_node("orchestrator", orchestrator_node)
    builder.add_node("ba_requirements_agent", requirements_node)
    builder.add_node("bpmn_agent", bpmn_node)
    builder.add_node("compiler_agent", compiler_node)

    graph = builder.compile()

    initial_state = {
        "input": task,
        "task": task,
        "messages": []
    }

    sa_graph_result = graph.invoke(initial_state)
    
