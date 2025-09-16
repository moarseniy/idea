from agents.langchain.builder import LangChainBuilder

builder = LangChainBuilder()
ba_agent_chain = builder.create_ba_chain()
sa_agent_chain = builder.create_sa_chain()
bpmn_agent_chain = builder.create_bpmn_chain()

task = """Разработать сервис, который поможет аналитикам формировать диаграммы процессов.

- Аналитик описывает процесс голосом;
- Система генерирует диаграмму и отображает ее аналитику;
- Система в режиме чата с аналитиком вносит правки в диаграмму.
- Подразумеватся web-приложение.
"""

# with open("test_ba_task.txt", "r") as f:
#     task = f.read()

# with open("test_sa_task.txt", "r") as f:
#     ba_requirements = f.read()

initial_state = {
    "task": task,
}

result = bpmn_agent_chain.invoke(initial_state)
print(result)