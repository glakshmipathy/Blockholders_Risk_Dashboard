\#Future Risk Dashboard
The Future Risk Dashboard is a graph-based analytics application built with **Memgraph**, **Streamlit**, and **Google Gemini**. It allows users to visualize how risks propagate through corporate ownership structures, analyze portfolio-level risk metrics, and simulate "what-if" scenarios.

The application uses a knowledge graph to model relationships between **Blockholders**, **Companies**, and **Risk Factors**, providing a powerful way to understand complex, interconnected risks.

### ✨ Key Features

  * **Interactive Graph Visualization**: Explore ownership chains and inherited risks for any selected company or blockholder.
  * **Risk Analytics Dashboard**: View key insights like top riskiest companies, sectoral risk concentration, and total risk exposure.
  * **Scenario Analysis**: Simulate company acquisitions, divestitures, or risk events and instantly see the impact on your portfolio.
  * **Natural Language Query**: Use plain English to ask questions about the graph data, which are translated into Cypher queries by Google Gemini.

-----

### 🚀 Prerequisites

Before you begin, ensure you have the following software installed:

  * **Python 3.10+**: The core programming language for the application.
  * **Docker Desktop**: Required to run the Memgraph database.

-----

### 🔑 Setup: API Keys and Credentials

This project requires a connection to a Memgraph database and a Google Gemini API key for the NL Query feature.

#### 1\. Get Your Memgraph Database Credentials

1.  **Sign up for a free Memgraph Cloud account** at [Memgraph Cloud](https://cloud.memgraph.com/).
2.  Follow the on-screen instructions to create a new project and a database instance.
3.  Once your database is created, navigate to its details page. You will find the connection string and credentials.
4.  Note down your **Host**, **Port**, **Username**, and **Password**.

#### 2\. Get Your Google Gemini API Key

1.  **Go to Google AI Studio** at [Google AI Studio](https://aistudio.google.com/app/apikey).
2.  Sign in with your Google account.
3.  Click **"Create API key in a new project"** to get your free API key.
4.  Copy the generated key.

#### 3\. Create the `.env` File

In the root directory of this project, create a new file named `.env` and add your credentials from the previous steps.

```text
# Memgraph Database Credentials
MEMGRAPH_URI=bolt://<YOUR_MEMGRAPH_HOST>:<YOUR_MEMGRAPH_PORT>
MEMGRAPH_USER=YOUR_USERNAME
MEMGRAPH_PASSWORD=YOUR_PASSWORD

# Gemini API Key
GEMINI_API_KEY=YOUR_GEMINI_API_KEY
```

**Important:** Replace the placeholder values with your actual credentials. Do not commit this file to public repositories.

-----

### 🧑‍💻 Running the Application (Locally)

Here is a step-by-step guide to run your project locally, bypassing Docker Compose.

#### 1\. Set Up Your Python Environment

1.  Open your VS Code terminal in the project's root directory.

2.  Create and activate a virtual environment.

    ```bash
    python -m venv .venv
    .venv\Scripts\activate
    ```

3.  Install all required Python libraries using your `requirements.txt` file.

    ```bash
    pip install -r requirements.txt
    ```

#### 2\. Start Memgraph

You will run Memgraph as a container in the background.

1.  Open a new VS Code terminal.

2.  Run the following command to start Memgraph, ensuring all necessary ports are mapped.

    ```bash
    docker run --name my_memgraph_instance -p 7687:7687 -p 7444:7444 -p 3000:3000 memgraph/memgraph-platform:latest
    ```

#### 3\. Run the Data Pipeline

After Memgraph has fully started (it may take a minute), you can run your data pipeline. This step populates the database and prepares it for the application.

1.  In your Python virtual environment terminal, run the pipeline script.

    ```bash
    python run_pipeline.py
    ```

    This process can take several minutes to complete, especially if you have a large dataset. Wait for the terminal output to confirm that the pipeline is complete.

#### 4\. Launch the Streamlit App

Once the data pipeline is finished, you can launch the Streamlit application.

1.  In the same terminal where your virtual environment is active, run the following command.

    ```bash
    streamlit run app.py
    ```

    This will open the application in your default web browser.

-----

### 📁 Project Structure

```text
.
├── .env                  # Your environment variables (API keys, passwords)
├── Dockerfile            # Instructions to build the Streamlit application container
├── docker-compose.yml    # Defines and links the Streamlit and Memgraph containers
├── app.py                # Main Streamlit dashboard file
├── config.py             # Centralized project configuration variables
├── data/                 # Folder for CSV data files
│   ├── blockholders.csv
│   └── ...
├── modules/              # Python modules for core logic
│   ├── db_loader.py      # Handles data loading into Memgraph
│   ├── risk_engine.py    # The core risk propagation logic
│   ├── llm_utils.py      # Helpers for Gen AI queries
│   └── logging_utils.py  # Logging configuration
├── scripts/              # Scripts for generating mock data
│   ├── generate_market_cap.py
│   └── ...
├── visualizations/       # Code for rendering the graph and charts
│   ├── graph_renderer.py # Contains Pyvis graph rendering logic
│   └── ...
├── run_pipeline.py       # Main script to automate the entire data pipeline
└── requirements.txt      # Python dependencies
```