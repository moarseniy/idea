from abstract.singleton import Singleton

import os, re, json
from langchain.output_parsers.pydantic import PydanticOutputParser
from langchain_core.exceptions import OutputParserException
from langchain_huggingface import HuggingFaceEndpoint, HuggingFacePipeline, ChatHuggingFace
from langchain_openai import ChatOpenAI as CH

from agents.langchain.agent_responses import responses_types, hf_responses_types

a_hf_token ="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"
a_key = "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"

os.environ["OPENAI_API_KEY"] = a_key
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

        self.DA_TEMPLATE = self.__read_file("prompts/da/da_template.txt")
        self.DA_VALIDATOR_TEMPLATE = self.__read_file("prompts/da/da_validator_template.txt")

        self.DE_TEMPLATE = self.__read_file("prompts/de/de_template.txt")
        self.DE_VALIDATOR_TEMPLATE = self.__read_file("prompts/de/de_validator_template.txt")
        self.DE_CORRECTOR_TEMPLATE = self.__read_file("prompts/de/de_corrector_template.txt")
        self.DE_CORRECTOR_VALIDATOR_TEMPLATE = self.__read_file("prompts/de/de_corrector_validator_template.txt")
        self.DE_CORRECTOR_SYSTEM_TEMPLATE = self.__read_file("prompts/de/de_corrector_system_template.txt")
        self.DE_CORRECTOR_USER_TEMPLATE = self.__read_file("prompts/de/de_corrector_user_template.txt")

        self.DARCH_TEMPLATE = self.__read_file("prompts/darch/darch_template.txt")
        self.DARCH_VALIDATOR_TEMPLATE = self.__read_file("prompts/darch/darch_validator_template.txt")
        self.DARCH_CORRECTOR_TEMPLATE = self.__read_file("prompts/darch/darch_corrector_template.txt")
        self.DARCH_CORRECTOR_VALIDATOR_TEMPLATE = self.__read_file("prompts/darch/darch_corrector_validator_template.txt")
        self.DARCH_CORRECTOR_SYSTEM_TEMPLATE = self.__read_file("prompts/darch/darch_corrector_system_template.txt")
        self.DARCH_CORRECTOR_USER_TEMPLATE = self.__read_file("prompts/darch/darch_corrector_user_template.txt")
        
        self.DA_INSTRUCTION = self.__read_file("instructions/da_instruction.md")
        self.DE_INSTRUCTION = self.__read_file("instructions/de_instruction.md")
        self.DARCH_INSTRUCTION = self.__read_file("instructions/darch_instruction.md")

        self.langchain_llm_name = "gpt-4.1-mini"
        self.langchain_llm_max_name = "gpt-4.1-mini"
        self._temperature = 0.03

        self.to_use_hf = False
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
    def da_template(self):
        return self.DA_TEMPLATE

    @property
    def da_validator_prompt(self):
        return self.DA_VALIDATOR_TEMPLATE

    @property
    def de_template(self):
        return self.DE_TEMPLATE

    @property
    def de_validator_prompt(self):
        return self.DE_VALIDATOR_TEMPLATE
    
    @property
    def correction_de_agent_prompt(self):
        return self.DE_CORRECTOR_TEMPLATE

    @property
    def correction_de_validator_agent_prompt(self):
        return self.de_CORRECTOR_VALIDATOR_TEMPLATE

    @property
    def de_corrector_system_prompt(self):
        return self.DE_CORRECTOR_SYSTEM_TEMPLATE

    @property
    def de_corrector_user_prompt(self):
        return self.DE_CORRECTOR_USER_TEMPLATE

    @property
    def darch_template(self):
        return self.DARCH_TEMPLATE

    @property
    def darch_validator_prompt(self):
        return self.DARCH_VALIDATOR_TEMPLATE

    @property
    def correction_darch_agent_prompt(self):
        return self.DARCH_CORRECTOR_TEMPLATE

    @property
    def correction_darch_validator_agent_prompt(self):
        return self.DARCH_CORRECTOR_VALIDATOR_TEMPLATE

    @property
    def darch_corrector_system_prompt(self):
        return self.DARCH_CORRECTOR_SYSTEM_TEMPLATE

    @property
    def darch_corrector_user_prompt(self):
        return self.DARCH_CORRECTOR_USER_TEMPLATE
    
    @property
    def correction_orchestrator_prompt(self):
        return self.ORCHESTRATOR_TEMPLATE

    @property
    def da_instruction(self):
        return self.DA_INSTRUCTION

    @property
    def de_instruction(self):
        return self.DE_INSTRUCTION

    @property
    def darch_instruction(self):
        return self.DARCH_INSTRUCTION

    @property
    def temperature(self):
        return self._temperature
