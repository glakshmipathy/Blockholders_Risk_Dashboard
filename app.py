import sys
import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from gqlalchemy import Memgraph, Node, Relationship
import logging
from dotenv import load_dotenv
import google.generativeai as genai
import json

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import config
from modules.logging_utils import logger
from modules.risk_engine import RiskEngine
from modules.llm_utils import query_llm, explain_query_result, get_gemini_model
from visualizations.graph_renderer import render_graph_as_html
from modules.db_loader import DBLoader, Blockholder, Company, RiskFactor, OWNS, EXPOSED_TO

logger.setLevel(logging.INFO)
logger.info("Starting Streamlit application...")

load_dotenv()
if not config.LLM_ENABLED:
    st.error("❌ GEMINI_API_KEY not found in .env. LLM features will be disabled.")
    LLM_ENABLED = False
else:
    genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
    LLM_ENABLED = True

st.set_page_config(page_title="Future Risk Dashboard", layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
    /* Main container and background */
    .stApp {
        background-color: #1A1A2E;
        color: #EAEAEA;
    }

    /* Sidebar styling */
    .sidebar .sidebar-content {
        background-color: #0E1117;
    }

    /* Header text */
    h1, h2, h3, h4, h5, h6 {
        color: #BB86FC;
    }

    /* Button styling */
    .stButton>button {
        background-color: #03DAC6;
        color: #0E1117;
        font-weight: bold;
        border: 2px solid #03DAC6;
        transition: all 0.2s ease-in-out;
    }
    .stButton>button:hover {
        background-color: #018786;
        color: white;
        border-color: #018786;
    }

    /* Selectbox styling */
    .stSelectbox>div>div {
        background-color: #2D2F44;
        color: #EAEAEA;
        border: 1px solid #BB86FC;
    }

    /* Text input styling */
    .stTextInput>div>div>input {
        background-color: #2D2F44;
        color: #EAEAEA;
        border: 1px solid #BB86FC;
    }

    /* Expander styling */
    .stExpander {
        background-color: #2D2F44;
        border-radius: 10px;
        padding: 10px;
        margin-bottom: 20px;
    }
</style>
""", unsafe_allow_html=True)

st.title("🚀 Future Risk Dashboard")

@st.cache_resource(show_spinner="🔄 Initializing and computing latest portfolio risk metrics...")
def initialize_risk_engine():
    try:
        engine = RiskEngine(uri=config.MEMGRAPH_URI, user=config.MEMGRAPH_USER, password=config.MEMGRAPH_PASSWORD)
        engine.compute_total_risk(max_iterations=config.MAX_RISK_ITERATIONS)
        engine.dollarize_risk()
        return engine
    except Exception as e:
        logger.error("Error during initial risk computation.", exc_info=True)
        st.error(f"❌ Error during automatic risk computation: {e}. Please ensure Memgraph is running and data is loaded/enriched via `run_pipeline.py`.")
        st.stop()

if "risk_engine" not in st.session_state:
    st.session_state.risk_engine = initialize_risk_engine()
    if st.session_state.risk_engine:
        st.success("✅ Risk computation complete!")

@st.cache_data(show_spinner="Fetching company list...")
def get_company_list():
    if "risk_engine" not in st.session_state or not st.session_state.risk_engine:
        return {}
    try:
        session = st.session_state.risk_engine.driver.session()
        results = session.run("""
            MATCH (c:Company)
            RETURN c.id AS company_id, c.name AS company_name
            ORDER BY c.name
        """).data()
        return {row['company_name']: row['company_id'] for row in results}
    except Exception as e:
        logger.error("Error fetching company list for dropdowns.", exc_info=True)
        return {}

@st.cache_data(show_spinner="Fetching blockholder list...")
def get_blockholder_list():
    if "risk_engine" not in st.session_state or not st.session_state.risk_engine:
        return {}
    try:
        session = st.session_state.risk_engine.driver.session()
        results = session.run("""
            MATCH (b:Blockholder)
            RETURN b.id AS blockholder_id, b.name AS blockholder_name
            ORDER BY b.name
        """).data()
        return {row['blockholder_name']: row['blockholder_id'] for row in results}
    except Exception as e:
        logger.error("Error fetching blockholder list for dropdowns.", exc_info=True)
        return {}

@st.cache_data(show_spinner="Fetching risk factor list...")
def get_risk_factor_list():
    if "risk_engine" not in st.session_state or not st.session_state.risk_engine:
        return []
    try:
        session = st.session_state.risk_engine.driver.session()
        results = session.run("MATCH (r:RiskFactor) RETURN DISTINCT r.name AS name ORDER BY name").data()
        return [row['name'] for row in results]
    except Exception as e:
        logger.error(f"Error fetching risk factor list: {e}", exc_info=True)
        return {}

@st.cache_data(show_spinner="Fetching sector list...")
def get_sector_list():
    if "risk_engine" not in st.session_state or not st.session_state.risk_engine:
        return []
    try:
        session = st.session_state.risk_engine.driver.session()
        results = session.run("MATCH (c:Company) WHERE c.sector IS NOT NULL RETURN DISTINCT c.sector AS sector ORDER BY sector").data()
        return [row['sector'] for row in results]
    except Exception as e:
        logger.error(f"Error fetching sector list: {e}", exc_info=True)
        return {}

@st.cache_data(show_spinner="Fetching location list...")
def get_location_list():
    if "risk_engine" not in st.session_state or not st.session_state.risk_engine:
        return []
    try:
        session = st.session_state.risk_engine.driver.session()
        results = session.run("MATCH (c:Company) WHERE c.location IS NOT NULL RETURN DISTINCT c.location AS location ORDER BY location").data()
        return [row['location'] for row in results]
    except Exception as e:
        logger.error(f"Error fetching location list: {e}", exc_info=True)
        return {}

with st.sidebar:
    st.image("https://upload.wikimedia.org/wikipedia/en/thumb/9/9a/Mphasis_logo.svg/1200px-Mphasis_logo.svg.png", width=250)
    st.header("Navigation")
    
    selected_page = st.radio("Choose a section", ["📈 Risk Analytics", "📊 Company/Blockholder View", "🧪 Scenario Analysis", "💬 NL Query"])
    
    st.markdown("---")
    st.header("Actions")
    if st.button("🔄 Recalculate Risk Metrics"):
        st.cache_data.clear()
        st.cache_resource.clear()
        with st.spinner("Recalculating..."):
            st.session_state.risk_engine = initialize_risk_engine()
        st.success("Risk metrics recalculated!")
        
if selected_page == "📈 Risk Analytics":
    st.header("📈 Portfolio Risk Insights")
    
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("### 🏆 Top 10 Riskiest Companies")
        try:
            top_companies = st.session_state.risk_engine.driver.session().run("""
                MATCH (c:Company) WHERE c.dollarized_risk IS NOT NULL AND c.dollarized_risk > 0
                RETURN c.name AS Name, c.dollarized_risk AS DollarizedRisk
                ORDER BY DollarizedRisk DESC LIMIT 10
            """).data()
            if top_companies:
                df = pd.DataFrame(top_companies)
                df['DollarizedRisk_B'] = df['DollarizedRisk'] / 1_000_000_000
                fig = px.bar(df, x='Name', y='DollarizedRisk_B',
                             title='Top 10 Riskiest Companies by Dollarized Risk',
                             labels={'DollarizedRisk_B': 'Dollarized Risk ($ Billions)'},
                             color='DollarizedRisk_B',
                             color_continuous_scale=px.colors.sequential.Reds)
                st.plotly_chart(fig)
            else:
                st.info("No top companies by total risk found.")
        except Exception as e:
            st.error(f"❌ Error fetching top companies: {e}")

    with col2:
        st.markdown("### 🤝 Top 10 Riskiest Blockholders")
        try:
            top_blockholders = st.session_state.risk_engine.driver.session().run("""
                MATCH (b:Blockholder) WHERE b.dollarized_risk IS NOT NULL AND b.dollarized_risk > 0
                RETURN b.name AS Name, b.dollarized_risk AS DollarizedRisk
                ORDER BY DollarizedRisk DESC LIMIT 10
            """).data()
            if top_blockholders:
                df = pd.DataFrame(top_blockholders)
                df['DollarizedRisk_B'] = df['DollarizedRisk'] / 1_000_000_000
                fig = go.Figure(go.Funnel(
                    y = df['Name'],
                    x = df['DollarizedRisk_B'],
                    textinfo = "value+percent initial",
                    marker = {"color": "red"}
                ))
                fig.update_layout(title_text="Top 10 Riskiest Blockholders by Dollarized Risk",
                                 xaxis_title="Dollarized Risk ($ Billions)",
                                 yaxis_title="Blockholder Name")
                st.plotly_chart(fig)
            else:
                st.info("No top blockholders by total risk found.")
        except Exception as e:
            st.error(f"❌ Error fetching top blockholders: {e}")

    st.markdown("---")

    col3, col4 = st.columns(2)
    with col3:
        st.markdown("### 📊 Portfolio Risk Treemap")
        try:
            treemap_data_cursor = st.session_state.risk_engine.driver.session().run("""
                MATCH (c:Company)
                WHERE c.dollarized_risk IS NOT NULL AND c.dollarized_risk > 0 AND c.sector IS NOT NULL
                RETURN c.name AS Company, c.sector AS Sector, c.dollarized_risk AS DollarizedRisk
                ORDER BY DollarizedRisk DESC
            """).data()
            if treemap_data_cursor:
                treemap_df = pd.DataFrame(treemap_data_cursor)
                if not treemap_df.empty:
                    treemap_df['DollarizedRisk_B'] = treemap_df['DollarizedRisk'] / 1_000_000_000
                    fig = px.treemap(treemap_df,
                                     path=[px.Constant("Portfolio"), 'Sector', 'Company'],
                                     values='DollarizedRisk_B',
                                     color='DollarizedRisk_B',
                                     color_continuous_scale='RdYlGn_r',
                                     title="Portfolio Risk Breakdown by Sector and Company (Dollarized Billions)")
                    st.plotly_chart(fig)
                else:
                    st.info("No data available for treemap visualization.")
            else:
                st.info("No data available for treemap visualization.")
        except Exception as e:
            st.error(f"❌ Error generating treemap: {e}")

    with col4:
        st.markdown("### 📊 Total Exposure by Risk Factor")
        try:
            risk_factor_exposure_data = st.session_state.risk_engine.driver.session().run("""
                MATCH (c:Company)-[e:EXPOSED_TO]->(r:RiskFactor) WHERE c.dollarized_risk IS NOT NULL AND c.dollarized_risk > 0
                RETURN r.name AS RiskFactor, sum(c.dollarized_risk * coalesce(e.weight, 0)) AS TotalDollarizedExposure
                ORDER BY TotalDollarizedExposure DESC LIMIT 15
            """).data()
            if risk_factor_exposure_data:
                df_bar = pd.DataFrame(risk_factor_exposure_data)
                df_bar['TotalDollarizedExposure'] = pd.to_numeric(df_bar['TotalDollarizedExposure'], errors='coerce').fillna(0)
                df_bar = df_bar[df_bar['TotalDollarizedExposure'] > 0]
                df_bar['TotalDollarizedExposure_B'] = df_bar['TotalDollarizedExposure'] / 1_000_000_000
                if not df_bar.empty:
                    fig_bar = px.bar(df_bar,
                                     x='RiskFactor',
                                     y='TotalDollarizedExposure_B',
                                     title='Total Portfolio Exposure to Top Risk Factors (Dollarized Billions)',
                                     labels={'TotalDollarizedExposure_B': 'Aggregated Exposure Value ($ Billions)'},
                                     color='TotalDollarizedExposure_B',
                                     color_continuous_scale=px.colors.sequential.Viridis)
                    st.plotly_chart(fig_bar)
                else:
                    st.info("No significant risk factor exposure data available (all exposures might be zero).")
            else:
                st.info("No risk factor exposure data available.")
        except Exception as e:
            st.error(f"❌ Error generating risk factor breakdown chart: {e}")

    st.markdown("---")

    col5, col6 = st.columns(2)
    with col5:
        st.markdown("### 🏭 Sectoral Concentration Risk")
        try:
            over_sectors = st.session_state.risk_engine.compute_sector_concentration(threshold=config.RISK_CONCENTRATION_THRESHOLD)
            if over_sectors:
                st.warning("⚠️ Overexposed sectors (share of total portfolio risk above 30% threshold):")
                st.table(pd.DataFrame(over_sectors).set_index("sector"))
            else:
                st.success("✅ No sectors exceed the concentration threshold.")
        except Exception as e:
            st.error(f"❌ Error computing sectoral concentration: {e}")
    
    with col6:
        st.markdown("### 🧠 Critical Nodes by Network Degree")
        try:
            critical_nodes = st.session_state.risk_engine.get_critical_nodes_by_degree(top_n=config.TOP_N_CRITICAL_NODES)
            if critical_nodes:
                st.table(pd.DataFrame(critical_nodes).set_index("name"))
            else:
                st.info("No critical nodes found based on network degree.")
        except Exception as e:
            st.error(f"❌ Error fetching critical nodes: {e}")


elif selected_page == "📊 Company/Blockholder View":
    company_name_to_id = get_company_list()
    company_names = sorted(list(company_name_to_id.keys()))
    blockholder_name_to_id = get_blockholder_list()
    blockholder_names = sorted(list(blockholder_name_to_id.keys()))

    selected_node_type = st.radio(
        "**Select node type to visualize:**",
        ["Company", "Blockholder"],
        index=0
    )
    
    selected_node_name = ""
    selected_node_id = ""

    if selected_node_type == "Company":
        selected_node_name = st.selectbox(
            "**Select a company to view its risk profile:**",
            [""] + company_names,
            index=0,
            help="Choose a company to visualize its network of owners and risks."
        )
        if selected_node_name:
            selected_node_id = company_name_to_id.get(selected_node_name)
    else: # Blockholder
        selected_node_name = st.selectbox(
            "**Select a blockholder to view its inherited risk profile:**",
            [""] + blockholder_names,
            index=0,
            help="Choose a blockholder to visualize its direct ownerships and the risks of those companies."
        )
        if selected_node_name:
            selected_node_id = blockholder_name_to_id.get(selected_node_name)

    if selected_node_name:
        st.markdown("### 🗂 Graph Legend")
        st.markdown("---")
        
        col1, col2 = st.columns(2)

        with col1:
            st.markdown(
                """
                **🎨 Node Colors:**<br>
                <span style='color:#e67e22;'>&#11044;</span> **High Risk** — Risk level > **66%** of max<br>
                <span style='color:#f1c40f;'>&#11044;</span> **Medium Risk** — Risk level between **34%–66%**<br>
                <span style='color:#27ae60;'>&#11044;</span> **Low Risk** — Risk level < **33%**<br>
                <span style='color:#7f8c8d;'>&#11044;</span> **No Risk** — Dollarized Risk = **$0**<br>
                <span style='color:#e74c3c;'>&#9670;</span> **Risk Factor** — Fixed color and diamond shape
                """,
                unsafe_allow_html=True
            )

        with col2:
            st.markdown(
                """
                **📏 Node Sizing:**<br>
                **Nodes** with higher **Dollarized Risk** appear larger.<br>
                **Risk Factor** size is based on total exposure.
                """,
                unsafe_allow_html=True
            )
        st.markdown("---")

        if selected_node_type == "Company":
            graph_cypher_query = f"""
                MATCH (c:Company {{id: '{selected_node_id}'}})
                OPTIONAL MATCH (bh:Blockholder)-[o:OWNS]->(c)
                OPTIONAL MATCH (c)-[o2:OWNS]->(owned_c:Company)
                OPTIONAL MATCH (owned_c)-[o3:OWNS]->(sub_owned_c:Company)
                OPTIONAL MATCH (c)-[e1:EXPOSED_TO]->(rf1:RiskFactor)
                OPTIONAL MATCH (owned_c)-[e2:EXPOSED_TO]->(rf2:RiskFactor)
                OPTIONAL MATCH (sub_owned_c)-[e3:EXPOSED_TO]->(rf3:RiskFactor)
                RETURN c, bh, o, owned_c, o2, sub_owned_c, o3, rf1, e1, rf2, e2, rf3, e3
                LIMIT 100
            """
        else: # Blockholder
            graph_cypher_query = f"""
                MATCH (bh:Blockholder {{id: '{selected_node_id}'}})
                OPTIONAL MATCH (bh)-[o1:OWNS]->(c1:Company)
                OPTIONAL MATCH (c1)-[o2:OWNS]->(c2:Company)
                OPTIONAL MATCH (c2)-[o3:OWNS]->(c3:Company)
                OPTIONAL MATCH (c1)-[e1:EXPOSED_TO]->(rf1:RiskFactor)
                OPTIONAL MATCH (c2)-[e2:EXPOSED_TO]->(rf2:RiskFactor)
                OPTIONAL MATCH (c3)-[e3:EXPOSED_TO]->(rf3:RiskFactor)
                RETURN bh, o1, c1, o2, c2, o3, c3, e1, rf1, e2, rf2, e3, rf3
                LIMIT 200
            """
        
        with st.spinner("🔄 Rendering personalized risk graph..."):
            try:
                html_content = render_graph_as_html(graph_cypher_query)
                st.components.v1.html(html_content, height=1200, width=1200, scrolling=True)
            except Exception as e:
                st.error(f"❌ Failed to render graph: {e}. Check console for details.")
    else:
        st.info("Please select a company or blockholder from the dropdown to visualize its risk profile.")


elif selected_page == "🧪 Scenario Analysis":
    st.header("🧪 Scenario Analysis & Simulation")
    
    company_name_to_id = get_company_list()
    company_names = sorted(list(company_name_to_id.keys()))
    risk_factor_names = get_risk_factor_list()
    sector_names = get_sector_list()
    location_names = get_location_list()

    scenario_type = st.radio("Select Scenario Type", ["Acquisition", "Risk Event Impact"])

    if st.session_state.get('last_scenario_type') != scenario_type:
        st.session_state.acquisition_results = None
        st.session_state.risk_event_results = None
    st.session_state.last_scenario_type = scenario_type

    if scenario_type == "Acquisition":
        st.subheader("🔗 Simulate Company Acquisition")
        col1, col2, col3 = st.columns(3)
        with col1:
            acquirer_name = st.selectbox("Acquiring Company Name", [""] + company_names, index=0, help="Select the company making the acquisition.")
            acquirer_id = company_name_to_id.get(acquirer_name)
        with col2:
            acquired_name = st.selectbox("Acquired Company Name", [""] + company_names, index=1, help="Select the company being acquired.")
            acquired_id = company_name_to_id.get(acquired_name)
        with col3:
            ownership_pct = st.number_input("Ownership Percentage (0.0 - 1.0)", min_value=0.0, max_value=1.0, value=0.5, step=0.05, format="%.2f")
        
        if st.button("Run Acquisition Scenario"):
            if acquirer_id and acquired_id and st.session_state.risk_engine:
                with st.spinner("Running acquisition scenario..."):
                    try:
                        st.session_state.risk_engine.export_snapshot("output/snapshot_before.json")
                        success = st.session_state.risk_engine.simulate_acquisition(acquirer_id, acquired_id, ownership_pct)
                        if success:
                            st.session_state.risk_engine.compute_total_risk()
                            st.session_state.risk_engine.dollarize_risk()
                            st.cache_data.clear()
                            st.cache_resource.clear()
                            st.session_state.risk_engine.export_snapshot("output/snapshot_after.json")
                            st.session_state.risk_engine.generate_diff("output/snapshot_before.json", "output/snapshot_after.json")
                            diff_df = pd.read_csv("output/diff_report.csv")
                            diff_df = diff_df[diff_df['delta'] != 0] # Filter for non-zero changes
                            llm_summary = "❌ LLM is not configured (GEMINI_API_KEY missing or invalid)."
                            if LLM_ENABLED and not diff_df.empty:
                                llm_summary = query_llm(f"Summarize the following risk changes after an acquisition. Focus on top gainers/losers in total risk. Data:\n{diff_df.to_string()}")
                            st.session_state.acquisition_results = {"status": "success", "diff_df": diff_df, "llm_summary": llm_summary}
                            st.rerun()
                        else:
                            st.session_state.acquisition_results = {"status": "error", "message": "Acquisition simulation failed. Check company IDs or console for details."}
                            st.rerun()
                    except Exception as e:
                        st.session_state.acquisition_results = {"status": "error", "message": f"Error during acquisition scenario: {e}"}
                        st.rerun()

        if st.session_state.get('acquisition_results') and st.session_state.acquisition_results["status"] == "success":
            results = st.session_state.acquisition_results
            st.success("Acquisition simulated successfully!")
            diff_df = results["diff_df"]
            if not diff_df.empty:
                st.subheader("📈 Risk Changes After Acquisition")
                st.markdown("**LLM Summary:**")
                st.markdown(results["llm_summary"])
                st.dataframe(diff_df)
                st.download_button("📥 Download Risk Changes CSV", diff_df.to_csv(index=False).encode('utf-8'), file_name="acquisition_risk_changes.csv")
            else:
                st.info("No significant risk changes detected after acquisition. Graph state may have updated.")
        elif st.session_state.get('acquisition_results') and st.session_state.acquisition_results["status"] == "error":
            st.error(f"❌ {st.session_state.acquisition_results['message']}")

    elif scenario_type == "Risk Event Impact":
        st.subheader("💥 Simulate Risk Event Impact")
        
        selected_risk_factor = st.selectbox("Select Risk Factor", [""] + risk_factor_names)
        impact_multiplier = st.number_input("Impact Multiplier (e.g., 1.5 for a 50% increase)", min_value=0.0, max_value=2.0, value=1.5, step=0.1)
        
        with st.expander("Target Specific Companies, Sectors, or Locations"):
            target_company = st.selectbox("Target Company (optional)", [""] + company_names, index=0)
            target_sector = st.selectbox("Target Sector (optional)", [""] + sector_names, index=0)
            target_location = st.selectbox("Target Location (optional)", [""] + location_names, index=0)

        if st.button("Run Risk Event Scenario"):
            if selected_risk_factor:
                with st.spinner("Running risk event scenario..."):
                    try:
                        st.session_state.risk_engine.export_snapshot("output/snapshot_before.json")
                        
                        updated_count = st.session_state.risk_engine.simulate_risk_event(
                            risk_factor_name=selected_risk_factor,
                            impact_multiplier=impact_multiplier,
                            target_company_id=company_name_to_id.get(target_company),
                            target_sector=target_sector if target_sector else None,
                            target_location=target_location if target_location else None
                        )
                        if updated_count > 0:
                            st.session_state.risk_engine.compute_total_risk()
                            st.session_state.risk_engine.dollarize_risk()
                            st.cache_data.clear()
                            st.cache_resource.clear()
                            st.session_state.risk_engine.export_snapshot("output/snapshot_after.json")
                            st.session_state.risk_engine.generate_diff("output/snapshot_before.json", "output/snapshot_after.json")
                            diff_df = pd.read_csv("output/diff_report.csv")
                            diff_df = diff_df[diff_df['delta'] != 0] # Filter for non-zero changes
                            llm_summary = "❌ LLM is not configured (GEMINI_API_KEY missing or invalid)."
                            if LLM_ENABLED and not diff_df.empty:
                                llm_summary = query_llm(f"Summarize the following risk changes after a risk event. Focus on top gainers/losers in total risk. Data:\n{diff_df.to_string()}")
                            st.session_state.risk_event_results = {"status": "success", "diff_df": diff_df, "llm_summary": llm_summary}
                            st.rerun()
                        else:
                            st.session_state.risk_event_results = {"status": "info", "message": "No companies were exposed to the selected risk factor, or the event had no effect."}
                            st.rerun()
                    except Exception as e:
                        st.session_state.risk_event_results = {"status": "error", "message": f"Error during risk event scenario: {e}"}
                        st.rerun()
            else:
                st.warning("Please select a risk factor to run the simulation.")

        if st.session_state.get('risk_event_results') and st.session_state.risk_event_results["status"] == "success":
            results = st.session_state.risk_event_results
            st.success("Risk event simulated successfully!")
            diff_df = results["diff_df"]
            if not diff_df.empty:
                st.subheader("📈 Risk Changes After Event")
                st.markdown("**LLM Summary:**")
                st.markdown(results["llm_summary"])
                st.dataframe(diff_df)
                st.download_button("📥 Download Risk Changes CSV", diff_df.to_csv(index=False).encode('utf-8'), file_name="risk_event_changes.csv")
            else:
                st.info("No significant risk changes detected after the event.")
        elif st.session_state.get('risk_event_results') and st.session_state.risk_event_results["status"] == "error":
            st.error(f"❌ {st.session_state.risk_event_results['message']}")
        elif st.session_state.get('risk_event_results') and st.session_state.risk_event_results["status"] == "info":
            st.info(f"ℹ️ {st.session_state.risk_event_results['message']}")
            
elif selected_page == "💬 NL Query":
    st.header("💬 Ask a Question about the Graph")
    query_text = st.text_input("e.g., 'Which companies have total risk > 0.5?'")
    if st.button("🔍 Run Query"):
        if not LLM_ENABLED:
            st.warning("LLM features are disabled because GEMINI_API_KEY is missing.")
        elif not query_text.strip():
            st.warning("Please enter a question.")
        else:
            with st.spinner("Generating Cypher query and running..."):
                schema_prompt = """
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
- `(p:Blockholder)-[o:OWNS]->(c:Company)`: `p` owns `c`. The relationship has a `percent` property (0.0-1.0), `year`.
- `(c:Company)-[e:EXPOSED_TO]->(rf:RiskFactor)`: `c` is exposed to `rf`. The relationship has a `weight` property (0.0-1.0).
# Business Logic & Best Practices:
- A `Company`'s `direct_risk` is the sum of all `e.weight` values from its outgoing `EXPOSED_TO` relationships.
- A `Blockholder`'s `total_risk` is a sum of its own `direct_risk` (if it were a company) plus the risks inherited from the companies it owns. The inherited risk is calculated as the owned company's `total_risk` multiplied by the ownership `o.percent`.
- `dollarized_risk` is calculated by multiplying `total_risk` by `market_cap`.
- To make queries more robust and handle case sensitivity, use `toLower()` for string matching, for example: `WHERE toLower(c.location) = 'new york'`.
- To prevent errors with null values, use `coalesce(property, 0)`.
- **Always use `id` or `name` for node identification.** `id` is a unique identifier, and `name` is the human-readable label. The user's query may use either.
- **For queries asking about what a company or blockholder owns, match from the `name` property and traverse the outgoing `OWNS` relationship.**
- **For queries asking about who owns a company, match from the company's `name` property and traverse the incoming `OWNS` relationship.**
# Few-Shot Examples (Question -> Correct Query):
Question: "Which companies are owned by MORGAN STANLEY?"
MATCH (bh:Blockholder {name: "MORGAN STANLEY"})-[o:OWNS]->(c:Company) RETURN c.name AS OwnedCompany, o.percent AS OwnershipPercentage
Question: "Who owns TIGER GLOBAL MANAGEMENT LLC?"
MATCH (c:Company {name: "TIGER GLOBAL MANAGEMENT LLC"})<-[o:OWNS]-(bh:Blockholder) RETURN bh.name AS OwningBlockholder, o.percent AS OwnershipPercentage
Question: "Which companies in the Energy sector have a total risk higher than 0.8?"
MATCH (c:Company) WHERE c.sector = "Energy" AND coalesce(c.total_risk, 0) > 0.8 RETURN c.name AS Company, c.total_risk AS TotalRisk
Question: "List the top 5 riskiest companies."
MATCH (c:Company) RETURN c.name AS Company, c.total_risk AS TotalRisk ORDER BY TotalRisk DESC LIMIT 5
Question: "What is the total dollarized risk for 'MORGAN STANLEY'?"
MATCH (bh:Blockholder {name: "MORGAN STANLEY"}) RETURN bh.dollarized_risk AS TotalDollarizedRisk
Question: "Which companies contribute most to the risk of 'MORGAN STANLEY'?"
MATCH (b:Blockholder {name: "MORGAN STANLEY"})-[o:OWNS]->(c:Company) RETURN c.name AS Company, (coalesce(c.dollarized_risk, 0) * coalesce(o.percent,0)) AS ContributedDollarizedRisk ORDER BY ContributedDollarizedRisk DESC
Question: "Find all blockholders who own a stake in both Apple Inc. and Microsoft Corp."
MATCH (b:Blockholder)-[:OWNS]->(c1:Company {name: 'Apple Inc.'}), (b)-[:OWNS]->(c2:Company {name: 'Microsoft Corp.'}) RETURN DISTINCT b.name AS BlockholderName
Now, translate the following natural language question into a valid, standalone Cypher query. Do not include any comments or extra text outside of the query itself.
"""
                try:
                    model_for_query = get_gemini_model()
                    if model_for_query:
                        prompt_with_instructions = schema_prompt + f'"{query_text.strip()}"'
                        response_text = model_for_query.generate_content(prompt_with_instructions).text
                        cypher_query = response_text.strip().lstrip("```cypher").rstrip("```").strip()
                        st.code(cypher_query, language="cypher")
                        
                        memgraph_client = Memgraph()
                        results = memgraph_client.execute_and_fetch(cypher_query)
                        
                        # --- FIX: Check for results before creating DataFrame ---
                        if results:
                            df = pd.DataFrame(results)
                            if df.empty:
                                st.info("Query returned no results.")
                            else:
                                explanation = explain_query_result(cypher_query, df.head())
                                st.markdown("#### 📝 Summary of Results")
                                st.markdown(explanation)
                                st.markdown("#### 📊 Query Result")
                                st.dataframe(df)
                                st.download_button("📥 Download CSV", df.to_csv(index=False).encode('utf-8'), file_name="query_results.csv")
                        else:
                            st.info("Query returned no results or there was a connection error.")
                        # --- END FIX ---
                    else:
                        st.warning("LLM model could not be initialized. Check API Key.")
                except Exception as e:
                    st.error(f"❌ Error running LLM-generated query or processing results: {e}")
                    st.info("Possible issues: Invalid Cypher from LLM, Memgraph connection, or data error.")

st.markdown("---")
st.caption("Developed for Mphasis.ai, portfolio graph analytics using Memgraph, Streamlit")