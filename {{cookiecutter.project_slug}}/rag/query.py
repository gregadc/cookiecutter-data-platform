import psycopg2
# from langchain_ollama import OllamaEmbeddings, ChatOllama
from langchain_ollama import OllamaEmbeddings
from langchain_anthropic import ChatAnthropic

DB_CONFIG = {
    "host": "localhost",
    "port": 5434,
    "dbname": "admin",
    "user": "admin",
    "password": "admin",
}


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def get_embeddings_model():
    return OllamaEmbeddings(model="nomic-embed-text")


def get_llm():
    # return ChatOllama(model="llama3.2:3b")
    return ChatAnthropic(
        model="claude-haiku-4-5-20251001",
        api_key="",
    )


def search_similar(conn, query_embedding, limit=5):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT content, metadata
            FROM embeddings
            ORDER BY embedding <-> %s::vector
            LIMIT %s
        """, (query_embedding, limit))
        return cur.fetchall()


def build_prompt(question, chunks):
    context = "\n".join([row[0] for row in chunks])
    return f"""Build prompt"""


def query(question: str):
    conn = get_connection()
    embeddings_model = get_embeddings_model()
    llm = get_llm()

    # 1. transform question to embedding
    query_embedding = embeddings_model.embed_query(question)

    # 2. search les similars chunks
    chunks = search_similar(conn, query_embedding)
    print(f"{len(chunks)} chunks found")

    # 3. build prompt and send to LLM
    prompt = build_prompt(question, chunks)
    response = llm.invoke(prompt)

    conn.close()
    print(f"Response: {response.content}")
    return response.content

if __name__ == "__main__":
    query("What was the closing price of Bitcoin ?")
