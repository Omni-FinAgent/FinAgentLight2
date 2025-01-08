from dotenv import load_dotenv
load_dotenv()

from finagentlight import CodeAgent, LiteLLMModel

model = LiteLLMModel(
    model_id="anthropic/claude-3-5-sonnet-20240620",
)
agent = CodeAgent(tools=[], model=model)

agent.run("Write a queue in Python and save it to the file `queue.py`.")