from api import agent_api
import asyncio
from random import randint

async def f(x):
    print(f"Task: {x}")
    delay = randint(0,10)
    await asyncio.sleep(delay)
    return x

async def run():
    tasks = [
        f(i) for i in range(10)
    ]
    R = await asyncio.gather(*tasks)
    return R

if __name__ == "__main__":

    # results = asyncio.run(run())
    # print(results)
    test = {

        "jsonSchema": {"schema":"schema"},
        "businessRequirements": "businessRequirements",
        "bmpnSchema": "bmpnSchema"
    }
    t = agent_api.AgentResponse(**test)
    print(t)