from abstract.singleton import Singleton

import os, re, json
from langchain.output_parsers.pydantic import PydanticOutputParser
from langchain_core.exceptions import OutputParserException
from langchain_huggingface import HuggingFaceEndpoint, HuggingFacePipeline, ChatHuggingFace
from langchain_openai import ChatOpenAI as CH

from agents.langchain.agent_responses import responses_types, hf_responses_types

a_hf_token ="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"
a_key = "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"
v_key = "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"

os.environ["OPENAI_API_KEY"] = v_key
os.environ["HUGGINGFACEHUB_API_TOKEN"] = a_hf_token

import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

def get_structured_response(llm, request, response_type) -> dict:
    parser = PydanticOutputParser(pydantic_object=response_type)
    
    # Улучшенный промпт с явным указанием формата
    fmt = parser.get_format_instructions()
    prompt = f"""
    Требования к ответу:
    1. Строго следуй схеме: {fmt}
    2. Ответ должен быть ВАЛИДНЫМ JSON
    3. Не добавляй поясняющий текст вокруг JSON

    Запрос: {request}
    """
    #     Пример правильного ответа:
    # {json.dumps(response_type.schema()['example'], indent=2, ensure_ascii=False)}
    try:
        # Получаем сырой ответ
        raw_response = llm.invoke(prompt)
        print(f"raw_response:{raw_response}")
        # Очистка ответа
        cleaned_response = re.sub(r'^.*?\{', '{', raw_response, flags=re.DOTALL)
        cleaned_response = re.sub(r'\}.*?$', '}', cleaned_response, flags=re.DOTALL)
        
        print(f"cleaned_response:{cleaned_response}")
        # exit(-1)
        # Парсинг
        return parser.parse(cleaned_response).dict()
    
    except (OutputParserException, json.JSONDecodeError) as e:
        # Fallback: попытка извлечь JSON из ответа
        try:
            json_str = re.search(r'\{.*\}', raw_response, re.DOTALL).group()
            return parser.parse(json_str)
        except:
            raise ValueError(f"Не удалось распарсить ответ модели: {raw_response}") from e

class AgentSettings(Singleton):
    def _setup(self):
        self.ORCHESTRATOR_TEMPLATE = self.__read_file("prompts/orchestrator_template.txt")

        self.BA_TEMPLATE = self.__read_file("prompts/ba/ba_template.txt")
        self.BA_CORRECTOR_TEMPLATE = self.__read_file("prompts/ba/ba_corrector_template.txt")
        self.SA_ANALITIC_TEMPLATE = self.__read_file("prompts/sa/sa_analitic_template.txt")
        self.SA_GENERATOR_TEMPLATE = self.__read_file("prompts/sa/sa_generator_template.txt")
        self.SA_CORRECTOR_TEMPLATE = self.__read_file("prompts/sa/sa_corrector_template.txt")

        self.BPMN_TEMPLATE = self.__read_file("prompts/bpmn/bpmn_template.txt")
        self.BPMN_DESCRIBER_TEMPLATE = self.__read_file("prompts/bpmn/bpmn_describer_template.txt")
        self.BPMN_CORRECTOR_TEMPLATE = self.__read_file("prompts/bpmn/bpmn_corrector_template.txt")

        self.JSON_TEMPLATE = self.__read_file("prompts/json/json_template.txt")

        self.BA_VALIDATOR_TEMPLATE = self.__read_file("prompts/ba/ba_validator_template.txt")
        self.BA_CORRECTOR_VALIDATOR_TEMPLATE = self.__read_file("prompts/ba/ba_corrector_validator_template.txt")

        self.SA_VALIDATOR_TEMPLATE = self.__read_file("prompts/sa/sa_validator_template.txt")
        self.SA_CORRECTOR_VALIDATOR_TEMPLATE = self.__read_file("prompts/sa/sa_corrector_validator_template.txt")

        self.BPMN_VALIDATOR_TEMPLATE = self.__read_file("prompts/bpmn/bpmn_validator_template.txt")
        self.BPMN_CORRECTOR_VALIDATOR_TEMPLATE = self.__read_file("prompts/bpmn/bpmn_corrector_validator_template.txt")


        self.BA_CORRECTOR_SYSTEM_TEMPLATE = self.__read_file("prompts/ba/ba_corrector_system_template.txt")
        self.BA_CORRECTOR_USER_TEMPLATE = self.__read_file("prompts/ba/ba_corrector_user_template.txt")
        self.SA_CORRECTOR_SYSTEM_TEMPLATE = self.__read_file("prompts/sa/sa_corrector_system_template.txt")
        self.SA_CORRECTOR_USER_TEMPLATE = self.__read_file("prompts/sa/sa_corrector_user_template.txt")
        self.BPMN_CORRECTOR_SYSTEM_TEMPLATE = self.__read_file("prompts/bpmn/bpmn_corrector_system_template.txt")
        self.BPMN_CORRECTOR_USER_TEMPLATE = self.__read_file("prompts/bpmn/bpmn_corrector_user_template.txt")
        self.JSON_CORRECTOR_SYSTEM_TEMPLATE = self.__read_file("prompts/json/json_corrector_system_template.txt")
        self.JSON_CORRECTOR_USER_TEMPLATE = self.__read_file("prompts/json/json_corrector_user_template.txt")

        self.BA_INSTRUCTION = self.__read_file("instructions/ba_instruction.md")
        self.SA_INSTRUCTION = self.__read_file("instructions/sa_instruction.md")
        self.BPMN_INSTRUCTION = self.__read_file("instructions/bpmn_instruction.md")
        self.JSON_INSTRUCTION = self.__read_file("instructions/json_instruction.md")

        self.langchain_llm_name = "gpt-4.1-mini"
        self.langchain_llm_max_name = "gpt-4.1-mini"
        self._temperature = 0.03

        self.to_use_hf = True
        self.to_use_local = False

        self.hf_deepseek = "deepseek-ai/DeepSeek-R1-Distill-Qwen-32B"
        self.hf_mistral = "mistralai/Mistral-7B-Instruct-v0.3"
        self.hf_shlyapa = "t-bank-ai/ruDialoGPT-small"

        self.hf_model = self.hf_deepseek

        self.hf_deepseek_endpoint_a100 = "https://vjja9kc60zyaaj2z.us-east-1.aws.endpoints.huggingface.cloud"
        self.hf_deepseek_endpoint = "https://s2zxmc9d1vf5ysm8.us-east4.gcp.endpoints.huggingface.cloud"
        self.hf_mistral_endpoint = "https://k2w2yjf5bgs8buzy.us-east-1.aws.endpoints.huggingface.cloud"
        
        self.hf_endpoint = self.hf_deepseek_endpoint

        self.vectorstore = "faiss_index"

        self.smolagents_llm_name = ""
        self.smolagents_llm_max_name = ""

    def select_model(self):
        if self.to_use_local and self.to_use_hf:
            return HuggingFacePipeline.from_model_id(
                model_id="llm/T-pro-it-1.0",
                task="text-generation",
                pipeline_kwargs={
                    "max_new_tokens": 2048,
                    "temperature": self.temperature,
                    "timeout": 600
                }
            )
        elif self.to_use_hf:
            # Think about ChatHuggingFace
            llm = HuggingFaceEndpoint(
                task="text-generation",
                endpoint_url=self.hf_endpoint,
                # repo_id=self.hf_model,
                max_new_tokens=2048,
                do_sample=False,
                temperature=self.temperature,
                return_full_text=False, # для парсинга
                timeout=600
            )
            return ChatHuggingFace(llm=llm)
        else:
            return CH(
                model=self.langchain_llm_name,
                temperature=self.temperature
            )

    def call_llm(self, llm, request, response_type):
        if self.to_use_hf:
            return get_structured_response(llm, request, hf_responses_types[response_type])
        else:
            return llm.with_structured_output(responses_types[response_type]).invoke(request)

    def __read_file(self, filename):
        with open(filename, "r") as f:
            return f.read()
    @property
    def ba_template(self):
        return self.BA_TEMPLATE

    @property
    def sa_analitic_template(self):
        return self.SA_ANALITIC_TEMPLATE

    @property
    def sa_generator_template(self):
        return self.SA_GENERATOR_TEMPLATE

    @property
    def bpmn_template(self):
        return self.BPMN_TEMPLATE

    @property
    def bpmn_describer_template(self):
        return self.BPMN_DESCRIBER_TEMPLATE

    @property
    def json_template(self):
        return self.JSON_TEMPLATE

    @property
    def ba_validator_prompt(self):
        return self.BA_VALIDATOR_TEMPLATE

    @property
    def sa_validator_prompt(self):
        return self.SA_VALIDATOR_TEMPLATE

    @property
    def bpmn_validator_prompt(self):
        return self.BPMN_VALIDATOR_TEMPLATE

    @property
    def ba_corrector_system_prompt(self):
        return self.BA_CORRECTOR_SYSTEM_TEMPLATE

    @property
    def ba_corrector_user_prompt(self):
        return self.BA_CORRECTOR_USER_TEMPLATE

    @property
    def sa_corrector_system_prompt(self):
        return self.SA_CORRECTOR_SYSTEM_TEMPLATE

    @property
    def sa_corrector_user_prompt(self):
        return self.SA_CORRECTOR_USER_TEMPLATE

    @property
    def bpmn_corrector_system_prompt(self):
        return self.BPMN_CORRECTOR_SYSTEM_TEMPLATE

    @property
    def bpmn_corrector_user_prompt(self):
        return self.BPMN_CORRECTOR_USER_TEMPLATE

    @property
    def json_corrector_system_prompt(self):
        return self.JSON_CORRECTOR_SYSTEM_TEMPLATE

    @property
    def json_corrector_user_prompt(self):
        return self.JSON_CORRECTOR_USER_TEMPLATE

    @property
    def ba_instruction(self):
        return self.BA_INSTRUCTION

    @property
    def sa_instruction(self):
        return self.SA_INSTRUCTION

    @property
    def bpmn_instruction(self):
        return self.BPMN_INSTRUCTION

    @property
    def json_instruction(self):
        return self.JSON_INSTRUCTION

    @property
    def correction_orchestrator_prompt(self):
        return self.ORCHESTRATOR_TEMPLATE

    @property
    def correction_ba_agent_prompt(self):
        return self.BA_CORRECTOR_TEMPLATE

    @property
    def correction_ba_validator_agent_prompt(self):
        return self.BA_CORRECTOR_VALIDATOR_TEMPLATE

    @property
    def correction_sa_agent_prompt(self):
        return self.SA_CORRECTOR_TEMPLATE

    @property
    def correction_sa_validator_agent_prompt(self):
        return self.SA_CORRECTOR_VALIDATOR_TEMPLATE

    @property
    def correction_bpmn_agent_prompt(self):
        return self.BPMN_CORRECTOR_TEMPLATE

    @property
    def correction_bpmn_validator_agent_prompt(self):
        return self.BPMN_CORRECTOR_VALIDATOR_TEMPLATE

    @property
    def temperature(self):
        return self._temperature
