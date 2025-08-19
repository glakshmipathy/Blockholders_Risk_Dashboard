import os
import pandas as pd
import google.generativeai as genai
from dotenv import load_dotenv

# Load API keys (from project root .env)
load_dotenv()

# Setup Gemini
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    print("ERROR: GEMINI_API_KEY not found in .env. LLM features will be disabled.")
else:
    genai.configure(api_key=GEMINI_API_KEY)

def get_gemini_model():
    """Returns a configured Gemini GenerativeModel instance."""
    if not GEMINI_API_KEY:
        return None # LLM is disabled
    try:
        return genai.GenerativeModel(model_name="gemini-1.5-flash")
    except Exception as e:
        print(f"ERROR: Failed to initialize Gemini model: {e}")
        return None

def query_llm(prompt: str, llm="gemini") -> str:
    """
    Sends a prompt to the specified LLM and returns the response.
    Currently supports 'gemini'.
    """
    if llm == "gemini":
        model = get_gemini_model()
        if not model:
            return "❌ LLM is not configured (GEMINI_API_KEY missing or invalid)."
        try:
            # Using a stateless send_message for single turns
            response = model.generate_content(prompt)
            if response.candidates and response.candidates[0].content:
                return response.candidates[0].content.parts[0].text.strip()
            return "❌ Gemini error: No content in response."
        except Exception as e:
            return f"❌ Gemini error: {e}"
    # Add other LLM implementations here if needed
    return "❌ Invalid LLM selected or not implemented."

def explain_query_result(query: str, preview_df: pd.DataFrame, llm="gemini") -> str:
    """
    Uses the LLM to explain the results of a Cypher query in business terms.
    """
    preview = preview_df.to_string(index=False)
    prompt = (
        f"You are a data analyst. A Cypher query has been run on a company risk knowledge graph.\n"
        f"Here is the query:\n{query}\n\n"
        f"Top rows of the result:\n{preview}\n\n"
        "Explain what this result shows in simple business terms (2–3 lines)."
    )
    return query_llm(prompt, llm=llm)

def create_cypher_query_from_llm(question: str) -> str:
    """
    Constructs a detailed prompt with schema and examples to get a precise Cypher query.
    """
    schema_and_examples_prompt = """
You are an expert Cypher engineer generating queries ONLY for Memgraph.
Your task is to translate a user's natural language question into a valid, standalone Cypher query.
Do not include any comments, explanations, or extra text outside of the query itself.

# Schema
- Node Labels: `Company`, `Blockholder`, `RiskFactor`.
- Relationship Types: `OWNS`, `EXPOSED_TO`.

# Node Properties:
- `Company`: `id`, `name`, `sector`, `location`, `total_risk`, `direct_risk`, `dollarized_risk`, `market_cap`.
- `Blockholder`: `id`, `name`, `type`, `total_risk`, `dollarized_risk`.
- `RiskFactor`: `name`.

# Relationship Properties:
- `(p:Blockholder)-[o:OWNS]->(c:Company)`: `percent` (0.0-1.0), `year`.
- `(c:Company)-[e:EXPOSED_TO]->(rf:RiskFactor)`: `weight` (0.0-1.0).

# Business Logic & Best Practices:
- `dollarized_risk` for a `Blockholder` is the sum of `(owned_company.dollarized_risk * ownership.percent)`.
- Use `coalesce(property, 0)` to handle null values gracefully.
- Use `toLower(property)` for case-insensitive string matching.
- Use `MERGE` to create and update nodes/relationships if they don't exist.
- Use `MATCH` to find existing data.

# Few-Shot Examples (Question -> Correct Query):
Question: "Which companies in the Energy sector have a total risk higher than 0.8?"
MATCH (c:Company) WHERE c.sector = "Energy" AND coalesce(c.total_risk, 0) > 0.8 RETURN c.name AS Company, c.total_risk AS TotalRisk

Question: "Does Berkshire Hathaway Inc. own Exxon Mobil?"
MATCH (b:Blockholder {name: "Berkshire Hathaway Inc."})-[o:OWNS]->(c:Company {name: "Exxon Mobil"}) RETURN o.percent AS OwnershipPercent

Question: "List the top 5 riskiest companies."
MATCH (c:Company) RETURN c.name AS Company, c.total_risk AS TotalRisk ORDER BY TotalRisk DESC LIMIT 5

Question: "What is the total dollarized risk for 'Gang Yu'?"
MATCH (bh:Blockholder {name: "Gang Yu"}) RETURN bh.dollarized_risk AS TotalDollarizedRisk

Question: "Which companies contribute most to the risk of 'Gang Yu'?"
MATCH (b:Blockholder {name: "Gang Yu"})-[o:OWNS]->(c:Company) RETURN c.name AS Company, (coalesce(c.dollarized_risk, 0) * coalesce(o.percent,0)) AS ContributedDollarizedRisk ORDER BY ContributedDollarizedRisk DESC

Question: "Find all blockholders who own a stake in both Apple Inc. and Microsoft Corp."
MATCH (b:Blockholder)-[:OWNS]->(c1:Company {name: 'Apple Inc.'}), (b)-[:OWNS]->(c2:Company {name: 'Microsoft Corp.'}) RETURN DISTINCT b.name AS BlockholderName
"""
    
    full_prompt = f"{schema_and_examples_prompt}\nQuestion: \"{question}\"\n"
    
    # Send the combined prompt to the LLM
    cypher_query_raw = query_llm(full_prompt)
    
    # Clean the output to ensure only the query remains
    if "```" in cypher_query_raw:
        cypher_query_cleaned = cypher_query_raw.strip().split("```cypher")[-1].strip().strip("```")
    else:
        cypher_query_cleaned = cypher_query_raw
        
    return cypher_query_cleaned

# Example usage within app.py
if __name__ == "__main__":
    # Simulate a user question
    user_question = "What is the total dollarized risk for Gang Yu, and which companies contribute the most to it?"
    
    # Get the Cypher query from the LLM
    generated_query = create_cypher_query_from_llm(user_question)
    
    # Print the result (this is what you'd pass to your Memgraph client)
    print("Generated Cypher Query:")
    print(generated_query)