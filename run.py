from dotenv import load_dotenv
load_dotenv()

from openai import OpenAI

client = OpenAI()

from finagentlight import CodeAgent, LiteLLMModel, DuckDuckGoSearchTool, VisitWebpageTool

model = LiteLLMModel(
    model_id="o1",
)
agent = CodeAgent(tools=[
    DuckDuckGoSearchTool, VisitWebpageTool
], model=model)

print(agent.run("How to make a website?"))