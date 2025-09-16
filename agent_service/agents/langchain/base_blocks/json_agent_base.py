from abstract.singleton import Singleton
from settings import AgentSettings

from langchain.agents import initialize_agent, AgentType, Tool
from langchain.memory import ConversationBufferMemory

from langchain_openai import ChatOpenAI as CH
from langchain_openai import OpenAIEmbeddings
from langchain.vectorstores import FAISS
from langchain.chains import RetrievalQA

class JsonAgentBuilder(Singleton):
    def _setup(self):
        self.settings = AgentSettings()

    def create_json_analitic_agent(self, memory=None):
        if not memory:
            memory = ConversationBufferMemory(memory_key="chat_history")
        embeddings = OpenAIEmbeddings()
        vectorstore = FAISS.load_local(self.settings.vectorstore, embeddings, allow_dangerous_deserialization=True)
        retriever = vectorstore.as_retriever()
        llm = self.settings.select_model()
        
        rag_chain = RetrievalQA.from_chain_type(
            llm=llm,
            retriever=retriever,
            return_source_documents=True
        )

        tools = [
            Tool(
                name="RAG",
                func=lambda q: rag_chain.invoke({"query": q})["result"],
                description="Используется для поиска правил составления json-схем."
            )
        ]
        analitic_agent = initialize_agent(
            tools=tools,
            llm=llm,
            memory=memory,
            agent=AgentType.CONVERSATIONAL_REACT_DESCRIPTION,
            verbose=True,
            handle_parsing_errors=True
        )
        return analitic_agent

    def create_json_corrector_agent(self):
        llm = self.settings.select_model()
        return llm
