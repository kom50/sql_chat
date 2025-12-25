import os
import json
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain_community.utilities import SQLDatabase
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage
from datetime import datetime
from pathlib import Path

load_dotenv()


class SQLChatWithPersistence:
    def __init__(self, max_history: int = 20, history_file: str = "chat_history.json"):
        self.llm = ChatOpenAI(
            openai_api_key=os.getenv("OPENROUTER_API_KEY"),
            openai_api_base="https://openrouter.ai/api/v1",
            model_name="openai/gpt-oss-20b:free",
            temperature=0,
            max_tokens=1024,
        )
        self.db = SQLDatabase.from_uri("sqlite:///db/store.db")
        self.max_history = max_history
        self.history_file = history_file
        self.chat_history = self.load_history()

        self.system_prompt = f"""You are a helpful SQL database assistant.
Database Schema:
{self.get_schema()}
"""

    def get_schema(self):
        return self.db.get_table_info()

    def load_history(self) -> list:
        """Load chat history from file"""
        if Path(self.history_file).exists():
            try:
                with open(self.history_file, "r") as f:
                    return json.load(f)
            except:
                return []
        return []

    def save_history(self):
        """Save chat history to file"""
        with open(self.history_file, "w") as f:
            json.dump(self.chat_history, f, indent=2)

    def run_sql(self, sql: str) -> str:
        """Execute SQL query safely"""
        sql = sql.strip()

        if "```sql" in sql.lower():
            sql = sql.split("```sql")[-1].split("```")[0]
        elif "```" in sql:
            sql = sql.split("```")[1].split("```")[0]

        lines = sql.strip().split("\n")
        for i, line in enumerate(lines):
            if (
                line.strip()
                .upper()
                .startswith(("SELECT", "INSERT", "UPDATE", "DELETE"))
            ):
                sql = "\n".join(lines[i:])
                break

        sql = sql.strip().rstrip(";") + ";"

        try:
            return self.db.run(sql)
        except Exception as e:
            return f"SQL Error: {e}"

    def build_context_messages(self) -> list:
        """Build message list with history for context"""
        messages = [SystemMessage(content=self.system_prompt)]

        # Add last N conversations as context
        for entry in self.chat_history[-5:]:
            messages.append(HumanMessage(content=entry["question"]))
            messages.append(AIMessage(content=entry["answer"]))

        return messages

    def ask(self, question: str) -> str:
        # Build context from history
        context_messages = self.build_context_messages()

        # Recent history summary for SQL generation
        recent_context = ""
        if self.chat_history:
            recent_context = "Recent queries:\n"
            for entry in self.chat_history[-3:]:
                recent_context += f"- Q: {entry['question']}\n"
                recent_context += f"  SQL: {entry['sql']}\n"

        # Step 1: Generate SQL
        sql_prompt = f"""Write ONLY the SQL query for this question. No explanation.

{recent_context}

Question: {question}
SQL:"""

        sql_messages = context_messages + [HumanMessage(content=sql_prompt)]
        sql_response = self.llm.invoke(sql_messages)
        sql_query = sql_response.content.strip()

        print(f"\n📝 SQL: {sql_query}")

        # Step 2: Execute
        result = self.run_sql(sql_query)
        print(f"📊 Result: {result}")

        # Step 3: Generate answer with context
        answer_prompt = f"""Question: {question}
SQL: {sql_query}
Result: {result}

Provide a clear, friendly answer:"""

        answer_messages = context_messages + [HumanMessage(content=answer_prompt)]
        answer = self.llm.invoke(answer_messages).content

        # Step 4: Save to history
        self.chat_history.append(
            {
                "timestamp": datetime.now().isoformat(),
                "question": question,
                "sql": sql_query,
                "result": str(result)[:500],
                "answer": answer,
            }
        )

        # Trim and save
        if len(self.chat_history) > self.max_history:
            self.chat_history = self.chat_history[-self.max_history :]

        self.save_history()

        return answer

    def clear_history(self):
        self.chat_history = []
        self.save_history()
        return "History cleared!"

    def show_history(self, limit: int = 5) -> str:
        if not self.chat_history:
            return "No history."

        output = "\n📜 Recent Conversations:\n" + "-" * 40 + "\n"
        for i, entry in enumerate(self.chat_history[-limit:], 1):
            output += f"\n[{i}] {entry['timestamp'][:19]}\n"
            output += f"Q: {entry['question']}\n"
            output += f"A: {entry['answer'][:100]}...\n"

        return output

    def search_history(self, keyword: str) -> str:
        """Search conversation history"""
        matches = []
        for entry in self.chat_history:
            if (
                keyword.lower() in entry["question"].lower()
                or keyword.lower() in entry["answer"].lower()
            ):
                matches.append(entry)

        if not matches:
            return f"No matches found for '{keyword}'"

        output = (
            f"\n🔍 Found {len(matches)} matches for '{keyword}':\n" + "-" * 40 + "\n"
        )
        for entry in matches[-5:]:
            output += f"\nQ: {entry['question']}\nA: {entry['answer'][:100]}...\n"

        return output


def main():
    chat = SQLChatWithPersistence(max_history=20)

    print("\n" + "=" * 50)
    print("🤖 SQL Chat with Persistent History")
    print("=" * 50)
    print(f"Tables: {chat.db.get_usable_table_names()}")
    print(f"Loaded {len(chat.chat_history)} previous conversations")
    print("\nCommands:")
    print("  quit          - Exit")
    print("  schema        - Show schema")
    print("  tables        - Show tables")
    print("  history       - Show recent history")
    print("  history all   - Show all history")
    print("  search <word> - Search history")
    print("  clear         - Clear history")
    print("=" * 50 + "\n")

    while True:
        question = input("You: ").strip()

        if not question:
            continue

        cmd = question.lower()

        if cmd == "quit":
            print("Goodbye!")
            break
        elif cmd == "schema":
            print(f"\n{chat.get_schema()}\n")
        elif cmd == "tables":
            print(f"\nTables: {chat.db.get_usable_table_names()}\n")
        elif cmd == "history":
            print(chat.show_history(5))
        elif cmd == "history all":
            print(chat.show_history(len(chat.chat_history)))
        elif cmd.startswith("search "):
            keyword = question[7:].strip()
            print(chat.search_history(keyword))
        elif cmd == "clear":
            print(f"\n{chat.clear_history()}\n")
        else:
            try:
                answer = chat.ask(question)
                print(f"\n🤖 {answer}\n")
            except Exception as e:
                print(f"\n❌ Error: {e}\n")


if __name__ == "__main__":
    main()
