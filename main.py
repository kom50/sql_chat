import os
import json
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain_community.utilities import SQLDatabase
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage
from datetime import datetime
from pathlib import Path
import time
import signal

load_dotenv()


class TimeoutException(Exception):
    pass


def timeout_handler(signum, frame):
    raise TimeoutException("Query execution timeout")


class SQLChatWithPersistence:
    def __init__(
        self,
        max_history: int = 20,
        history_file: str = "chat_history.json",
        max_rows: int = 100,
        query_timeout: int = 10,
    ):
        self.llm = ChatOpenAI(
            openai_api_key=os.getenv("OPENROUTER_API_KEY"),
            openai_api_base="https://openrouter.ai/api/v1",
            model_name="openai/gpt-4o-mini",
            temperature=0,
            max_tokens=1024,
        )
        self.db = SQLDatabase.from_uri("sqlite:///db/store.db")
        self.max_history = max_history
        self.history_file = history_file
        self.max_rows = max_rows  # Maximum rows to return
        self.query_timeout = query_timeout  # Query timeout in seconds
        self.chat_history = self.load_history()

        # Enhanced system prompt with production guidelines
        self.system_prompt = f"""You are a helpful SQL database assistant.

⚠️ CRITICAL RULES (MUST FOLLOW):
1. ALWAYS add LIMIT {self.max_rows} to SELECT queries (unless using COUNT/SUM/AVG)
2. Prefer aggregations (COUNT, SUM, AVG) over SELECT *
3. Always use WHERE clauses to filter data when possible
4. Use indexed columns for filtering: customer_id, product_id, id
5. Write ONLY the SQL query, no explanations or markdown
6. For large results, use TOP N or LIMIT queries

Database Schema:
{self.get_schema()}

Available Tables: {self.db.get_usable_table_names()}
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

    def validate_sql(self, sql: str) -> tuple[bool, str]:
        """
        Validate SQL query for safety and performance
        Returns: (is_valid, error_message)
        """
        sql_upper = sql.upper()

        # 1. Block dangerous operations
        dangerous_keywords = [
            "DROP",
            "TRUNCATE",
            "DELETE",
            "ALTER",
            "CREATE",
            "GRANT",
            "REVOKE",
        ]
        for keyword in dangerous_keywords:
            if keyword in sql_upper:
                return False, f"❌ '{keyword}' operations are not allowed for safety"

        # 2. Check for SELECT * without LIMIT
        if "SELECT *" in sql_upper or "SELECT\n*" in sql_upper:
            if (
                "LIMIT" not in sql_upper
                and "COUNT(*)" not in sql_upper
                and "COUNT (*)" not in sql_upper
            ):
                return (
                    False,
                    f"❌ SELECT * must include LIMIT {self.max_rows} to prevent loading too much data",
                )

        # 3. Check for LIMIT value
        if "SELECT" in sql_upper and "LIMIT" not in sql_upper:
            # Check if it's not an aggregation query
            if not any(
                agg in sql_upper for agg in ["COUNT(", "SUM(", "AVG(", "MAX(", "MIN("]
            ):
                return (
                    False,
                    f"⚠️ Please add LIMIT {self.max_rows} to your SELECT query",
                )

        # 4. Validate LIMIT doesn't exceed max_rows
        if "LIMIT" in sql_upper:
            try:
                # Extract limit value
                limit_part = sql_upper.split("LIMIT")[1].strip().split()[0]
                limit_value = int(limit_part.rstrip(";"))
                if limit_value > self.max_rows:
                    return (
                        False,
                        f"❌ LIMIT {limit_value} exceeds maximum allowed ({self.max_rows})",
                    )
            except:
                pass  # If parsing fails, let database handle it

        # 5. Check for multiple statements (SQL injection prevention)
        if sql.count(";") > 1:
            return False, "❌ Multiple SQL statements are not allowed"

        return True, ""

    def clean_sql(self, sql: str) -> str:
        """Clean and format SQL query"""
        sql = sql.strip()

        # Remove markdown code blocks
        if "```sql" in sql.lower():
            sql = sql.split("```sql")[-1].split("```")[0]
        elif "```" in sql:
            sql = sql.split("```")[1].split("```")[0]

        # Remove comments and explanations
        lines = sql.strip().split("\n")
        sql_lines = []
        for line in lines:
            line = line.strip()
            # Skip empty lines and comment lines
            if line and not line.startswith("--"):
                sql_lines.append(line)

        sql = " ".join(sql_lines)

        # Find the actual SQL statement
        sql_keywords = ["SELECT", "INSERT", "UPDATE", "DELETE", "WITH"]
        for keyword in sql_keywords:
            if keyword in sql.upper():
                idx = sql.upper().index(keyword)
                sql = sql[idx:]
                break

        # Ensure single semicolon at end
        sql = sql.strip().rstrip(";") + ";"

        return sql

    def auto_add_limit(self, sql: str) -> str:
        """Automatically add LIMIT if missing and appropriate"""
        sql_upper = sql.upper()

        # Don't add LIMIT if:
        # - Already has LIMIT
        # - Is an aggregation query
        # - Is UPDATE/INSERT/DELETE
        if "LIMIT" in sql_upper:
            return sql

        if any(agg in sql_upper for agg in ["COUNT(", "SUM(", "AVG(", "MAX(", "MIN("]):
            return sql

        if any(
            cmd in sql_upper for cmd in ["UPDATE", "INSERT", "DELETE", "CREATE", "DROP"]
        ):
            return sql

        # Add LIMIT for SELECT queries
        if "SELECT" in sql_upper:
            sql = sql.rstrip(";") + f" LIMIT {self.max_rows};"

        return sql

    def run_sql(self, sql: str) -> str:
        """Execute SQL query safely with timeout and validation"""
        # Step 1: Clean SQL
        sql = self.clean_sql(sql)
        print(f"🧹 Cleaned SQL: {sql}")

        # Step 2: Auto-add LIMIT
        sql = self.auto_add_limit(sql)
        if "LIMIT" in sql.upper():
            print(f"➕ Auto-added LIMIT: {sql}")

        # Step 3: Validate SQL
        is_valid, error_msg = self.validate_sql(sql)
        if not is_valid:
            return error_msg

        # Step 4: Execute with timeout
        try:
            # Set timeout alarm (Unix-based systems)
            if hasattr(signal, "SIGALRM"):
                signal.signal(signal.SIGALRM, timeout_handler)
                signal.alarm(self.query_timeout)

            start_time = time.time()
            result = self.db.run(sql)
            execution_time = time.time() - start_time

            # Cancel alarm
            if hasattr(signal, "SIGALRM"):
                signal.alarm(0)

            # Log execution time
            print(f"⏱️ Query executed in {execution_time:.2f}s")

            # Check result size
            result_str = str(result)
            if len(result_str) > 10000:
                print(
                    f"⚠️ Large result: {len(result_str)} characters ({len(result_str)//1000}KB)"
                )

            return result

        except TimeoutException:
            return f"❌ Query timeout ({self.query_timeout}s). Please add more filters or use aggregation."

        except Exception as e:
            return f"❌ SQL Error: {e}"

        finally:
            # Always cancel alarm
            if hasattr(signal, "SIGALRM"):
                signal.alarm(0)

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
            recent_context = "\n📚 Recent successful queries:\n"
            for entry in self.chat_history[-3:]:
                if not entry.get("result", "").startswith("❌"):
                    recent_context += f"- Q: {entry['question']}\n"
                    recent_context += f"  SQL: {entry['sql']}\n"

        # Step 1: Generate SQL
        sql_prompt = f"""Write ONLY the SQL query for this question.

IMPORTANT:
- Add LIMIT {self.max_rows} for SELECT queries (unless using COUNT/SUM/AVG)
- Use WHERE clauses to filter data
- Prefer aggregations over full table scans
- Return ONLY the SQL, no explanations

{recent_context}

Question: {question}
SQL:"""

        sql_messages = context_messages + [HumanMessage(content=sql_prompt)]

        try:
            sql_response = self.llm.invoke(sql_messages)
            sql_query = sql_response.content.strip()
        except Exception as e:
            return f"❌ LLM Error: {e}"

        print(f"\n📝 Generated SQL: {sql_query}")

        # Step 2: Execute with safety checks
        result = self.run_sql(sql_query)
        print(f"📊 Result: {result[:500]}...")  # Truncate for display

        # Check if query failed
        if str(result).startswith("❌"):
            # Save failed query to history
            self.chat_history.append(
                {
                    "timestamp": datetime.now().isoformat(),
                    "question": question,
                    "sql": sql_query,
                    "result": result,
                    "answer": result,
                    "status": "failed",
                }
            )
            self.save_history()
            return result

        # Step 3: Generate answer with context
        answer_prompt = f"""Question: {question}
SQL Query Used: {sql_query}
Database Result: {result}

Provide a clear, friendly, and accurate answer based on the data.
If the result is empty, explain that no matching records were found.
Format numbers and data in a readable way:"""

        answer_messages = context_messages + [HumanMessage(content=answer_prompt)]

        try:
            answer = self.llm.invoke(answer_messages).content
        except Exception as e:
            answer = f"Query succeeded but answer generation failed: {e}\n\nRaw result: {result}"

        # Step 4: Save to history
        self.chat_history.append(
            {
                "timestamp": datetime.now().isoformat(),
                "question": question,
                "sql": sql_query,
                "result": str(result)[:1000],  # Limit stored result size
                "answer": answer,
                "status": "success",
            }
        )

        # Trim and save
        if len(self.chat_history) > self.max_history:
            self.chat_history = self.chat_history[-self.max_history :]

        self.save_history()

        return answer

    def get_stats(self) -> str:
        """Get usage statistics"""
        if not self.chat_history:
            return "No queries yet."

        total = len(self.chat_history)
        successful = sum(1 for e in self.chat_history if e.get("status") != "failed")
        failed = total - successful

        output = f"""
📊 Query Statistics:
{'='*40}
Total Queries:      {total}
Successful:         {successful} ({successful/total*100:.1f}%)
Failed:             {failed} ({failed/total*100:.1f}%)
Max Rows Limit:     {self.max_rows}
Query Timeout:      {self.query_timeout}s
{'='*40}
"""
        return output

    def clear_history(self):
        self.chat_history = []
        self.save_history()
        return "✅ History cleared!"

    def show_history(self, limit: int = 5) -> str:
        if not self.chat_history:
            return "No history."

        output = "\n📜 Recent Conversations:\n" + "=" * 60 + "\n"
        for i, entry in enumerate(self.chat_history[-limit:], 1):
            status_icon = "✅" if entry.get("status") != "failed" else "❌"
            output += f"\n{status_icon} [{i}] {entry['timestamp'][:19]}\n"
            output += f"   Q: {entry['question']}\n"
            output += f"   SQL: {entry['sql'][:80]}...\n"
            output += f"   A: {entry['answer'][:100]}...\n"

        return output

    def search_history(self, keyword: str) -> str:
        """Search conversation history"""
        matches = []
        for entry in self.chat_history:
            if (
                keyword.lower() in entry["question"].lower()
                or keyword.lower() in entry.get("answer", "").lower()
                or keyword.lower() in entry.get("sql", "").lower()
            ):
                matches.append(entry)

        if not matches:
            return f"No matches found for '{keyword}'"

        output = (
            f"\n🔍 Found {len(matches)} matches for '{keyword}':\n" + "=" * 60 + "\n"
        )
        for entry in matches[-5:]:
            status_icon = "✅" if entry.get("status") != "failed" else "❌"
            output += f"\n{status_icon} Q: {entry['question']}\n"
            output += f"   SQL: {entry['sql'][:80]}...\n"
            output += f"   A: {entry.get('answer', 'N/A')[:100]}...\n"

        return output


def main():
    # Initialize with production settings
    chat = SQLChatWithPersistence(
        max_history=50,  # Keep last 50 queries
        max_rows=100,  # Maximum 100 rows per query
        query_timeout=10,  # 10 second timeout
    )

    print("\n" + "=" * 60)
    print("🤖 SQL Chat with Production Safety Features")
    print("=" * 60)
    print(f"📁 Database: store.db")
    print(f"📊 Tables: {chat.db.get_usable_table_names()}")
    print(f"💾 Loaded {len(chat.chat_history)} previous conversations")
    print(f"⚙️ Max rows per query: {chat.max_rows}")
    print(f"⏱️ Query timeout: {chat.query_timeout}s")
    print("\n📋 Commands:")
    print("  quit          - Exit")
    print("  schema        - Show database schema")
    print("  tables        - Show available tables")
    print("  history       - Show recent history (5)")
    print("  history all   - Show all history")
    print("  history <N>   - Show last N conversations")
    print("  search <word> - Search history")
    print("  stats         - Show query statistics")
    print("  clear         - Clear history")
    print("=" * 60 + "\n")

    while True:
        try:
            question = input("You: ").strip()

            if not question:
                continue

            cmd = question.lower()

            if cmd == "quit":
                print("👋 Goodbye!")
                break

            elif cmd == "schema":
                print(f"\n{chat.get_schema()}\n")

            elif cmd == "tables":
                print(f"\n📊 Tables: {chat.db.get_usable_table_names()}\n")

            elif cmd == "history":
                print(chat.show_history(5))

            elif cmd == "history all":
                print(chat.show_history(len(chat.chat_history)))

            elif cmd.startswith("history "):
                try:
                    n = int(question.split()[1])
                    print(chat.show_history(n))
                except:
                    print("❌ Usage: history <number>")

            elif cmd.startswith("search "):
                keyword = question[7:].strip()
                print(chat.search_history(keyword))

            elif cmd == "stats":
                print(chat.get_stats())

            elif cmd == "clear":
                confirm = input("⚠️ Clear all history? (yes/no): ").lower()
                if confirm == "yes":
                    print(f"\n{chat.clear_history()}\n")
                else:
                    print("❌ Cancelled\n")

            else:
                # Ask question
                print("\n🔄 Processing...\n")
                answer = chat.ask(question)
                print(f"\n🤖 {answer}\n")

        except KeyboardInterrupt:
            print("\n\n👋 Goodbye!")
            break

        except Exception as e:
            print(f"\n❌ Unexpected Error: {e}\n")


if __name__ == "__main__":
    main()
