import sys
import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from gqlalchemy import Memgraph, Node, Relationship
import logging
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import config
from modules.logging_utils import logger
from modules.risk_engine import RiskEngine
from modules.llm_utils import query_llm, explain_query_result, get_gemini_model
from visualizations.graph_renderer import render_graph_as_html
import google.generativeai as genai
import json
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
        return []

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
    st.image("https://www.memgraph.com/static/mg-logo-white-932c0211a774187f54c9354054a3903a.svg", width=250)
    st.header("Navigation")
    
    selected_page = st.radio("Choose a section", ["📊 Graph View", "📈 Risk Analytics", "🧪 Scenario Analysis", "💬 NL Query"])
    
    st.markdown("---")
    st.header("Actions")
    if st.button("🔄 Recalculate Risk Metrics"):
        st.cache_data.clear()
        st.cache_resource.clear()
        with st.spinner("Recalculating..."):
            st.session_state.risk_engine = initialize_risk_engine()
        st.success("Risk metrics recalculated!")

if selected_page == "📊 Graph View":
    
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
                <span style='color:#e67e22;'>&#11044;</span> <strong>High Risk</strong> — Risk level > <strong>66%</strong> of max<br>
                <span style='color:#f1c40f;'>&#11044;</span> <strong>Medium Risk</strong> — Risk level between <strong>34%–66%</strong><br>
                <span style='color:#27ae60;'>&#11044;</span> <strong>Low Risk</strong> — Risk level < <strong>33%</strong><br>
                <span style='color:#7f8c8d;'>&#11044;</span> <strong>No Risk</strong> — Dollarized Risk = <strong>$0</strong><br>
                <span style='color:#e74c3c;'>&#9670;</span> <strong>Risk Factor</strong> — Fixed color and diamond shape
                """,
                unsafe_allow_html=True
        )

        with col2:
            st.markdown(
                """
                **📏 Node Sizing:**<br>
                <strong>Companies</strong> and <strong>Blockholders</strong> → Larger size = Higher <strong>Dollarized Risk</strong><br>
                <strong>Risk Factors</strong> → Size reflects aggregated <strong>exposure percentage</strong>
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

elif selected_page == "📈 Risk Analytics":
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

elif selected_page == "🧪 Scenario Analysis":
    st.header("🧪 Scenario Analysis & Simulation")
    
    company_name_to_id = get_company_list()
    company_names = sorted(list(company_name_to_id.keys()))
    risk_factor_names = get_risk_factor_list()
    sector_names = get_sector_list()
    location_names = get_location_list()

    scenario_type = st.radio("Select Scenario Type", ["Acquisition", "Divestiture", "Risk Event Impact"])

    if st.session_state.get('last_scenario_type') != scenario_type:
        st.session_state.acquisition_results = None
        st.session_state.divestiture_results = None
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
            else:
                st.warning("Please provide both Acquiring and Acquired Company IDs.")

        if st.session_state.get('acquisition_results') and st.session_state.acquisition_results["status"] == "success":
            results = st.session_state.acquisition_results
            st.success("Acquisition simulated successfully!")
            diff_df = results["diff_df"]
            if not diff_df.empty:
                st.subheader("📈 Risk Changes After Acquisition")
                st.dataframe(diff_df)
                st.markdown("**LLM Summary:**")
                st.markdown(results["llm_summary"])
                st.download_button("📥 Download Risk Changes CSV", diff_df.to_csv(index=False).encode('utf-8'), file_name="acquisition_risk_changes.csv")
            else:
                st.info("No significant risk changes detected after acquisition. Graph state may have updated.")
        elif st.session_state.get('acquisition_results') and st.session_state.acquisition_results["status"] == "error":
            st.error(st.session_state.acquisition_results["message"])

    elif scenario_type == "Divestiture":
        st.subheader("✂️ Simulate Company Divestiture")
        col1, col2 = st.columns(2)
        with col1:
            divesting_name = st.selectbox("Divesting Company Name", [""] + company_names, index=0, help="Select the company divesting shares.")
            divesting_id = company_name_to_id.get(divesting_name)
        with col2:
            divested_name = st.selectbox("Divested Company Name", [""] + company_names, index=1, help="Select the company being divested.")
            divested_id = company_name_to_id.get(divested_name)
            
        if st.button("Run Divestiture Scenario"):
            if divesting_id and divested_id and st.session_state.risk_engine:
                with st.spinner("Running divestiture scenario..."):
                    try:
                        st.session_state.risk_engine.export_snapshot("output/snapshot_before.json")
                        success = st.session_state.risk_engine.simulate_divestiture(divesting_id, divested_id)
                        if success:
                            st.session_state.risk_engine.compute_total_risk()
                            st.session_state.risk_engine.dollarize_risk()
                            st.cache_data.clear()
                            st.cache_resource.clear()
                            st.session_state.risk_engine.export_snapshot("output/snapshot_after.json")
                            st.session_state.risk_engine.generate_diff("output/snapshot_before.json", "output/snapshot_after.json")
                            diff_df = pd.read_csv("output/diff_report.csv")
                            llm_summary = "❌ LLM is not configured (GEMINI_API_KEY missing or invalid)."
                            if LLM_ENABLED and not diff_df.empty:
                                llm_summary = query_llm(f"Summarize the following risk changes after a divestiture. Focus on top gainers/losers in total risk. Data:\n{diff_df.to_string()}")
                            st.session_state.divestiture_results = {"status": "success", "diff_df": diff_df, "llm_summary": llm_summary}
                            st.rerun()
                        else:
                            st.session_state.divestiture_results = {"status": "error", "message": "Divestiture simulation failed. Check company names or console for details."}
                            st.rerun()
                    except Exception as e:
                        st.session_state.divestiture_results = {"status": "error", "message": f"Error during divestiture scenario: {e}"}
                        st.rerun()
            else:
                st.warning("Please provide both Divesting and Divested Company names.")

        if st.session_state.get('divestiture_results') and st.session_state.divestiture_results["status"] == "success":
            results = st.session_state.divestiture_results
            st.success("Divestiture simulated successfully!")
            diff_df = results["diff_df"]
            if not diff_df.empty:
                st.subheader("📈 Risk Changes After Divestiture")
                st.dataframe(diff_df)
                st.markdown("**LLM Summary:**")
                st.markdown(results["llm_summary"])
                st.download_button("📥 Download Risk Changes CSV", diff_df.to_csv(index=False).encode('utf-8'), file_name="divestiture_risk_changes.csv")
            else:
                st.info("No significant risk changes detected after divestiture. Graph state may have updated.")
        elif st.session_state.get('divestiture_results') and st.session_state.divestiture_results["status"] == "error":
            st.error(st.session_state.divestiture_results["message"])

    elif scenario_type == "Risk Event Impact":
        st.subheader("🌪️ Simulate Risk Factor Impact")
        col1, col2 = st.columns(2)
        with col1:
            risk_factor_name = st.selectbox("Risk Factor Name", [""] + risk_factor_names, help="Select the risk factor to adjust. Must exist in graph.")
        with col2:
            impact_multiplier = st.number_input("Impact Multiplier (e.g., 1.2 for 20% increase, 0.8 for 20% decrease)", min_value=0.0, max_value=5.0, value=1.2, step=0.1, format="%.2f")

        target_option = st.radio("Apply risk event to:", ("Specific Company", "By Sector or Location", "All Companies with this Risk Factor"))

        target_company_id = None
        target_sector = None
        target_location = None

        if target_option == "Specific Company":
            target_company_name = st.selectbox("Target Company", [""] + company_names, help="Select a company to apply the risk event to.")
            target_company_id = company_name_to_id.get(target_company_name) if target_company_name else None
        elif target_option == "By Sector or Location":
            target_sector = st.selectbox("Target Sector", [""] + sector_names, help="Only apply impact to companies in this sector.")
            target_location = st.selectbox("Target Location", [""] + location_names, help="Only apply impact to companies in this location.")

        if st.button("Run Risk Event Scenario"):
            if risk_factor_name and st.session_state.risk_engine:
                if not target_company_id and not target_sector and not target_location and target_option != "All Companies with this Risk Factor":
                    st.warning("Please select a target for the risk event, or choose 'All Companies with this Risk Factor'.")
                else:
                    with st.spinner("Running risk event scenario..."):
                        try:
                            st.session_state.risk_engine.export_snapshot("output/snapshot_before.json")
                            updated_count = st.session_state.risk_engine.simulate_risk_event(
                                risk_factor_name,
                                impact_multiplier,
                                target_company_id=target_company_id,
                                target_sector=target_sector,
                                target_location=target_location
                            )
                            
                            if updated_count > 0:
                                st.session_state.risk_engine.compute_total_risk()
                                st.session_state.risk_engine.dollarize_risk()
                                st.cache_data.clear()
                                st.cache_resource.clear()
                                st.session_state.risk_engine.export_snapshot("output/snapshot_after.json")
                                st.session_state.risk_engine.generate_diff("output/snapshot_before.json", "output/snapshot_after.json")
                                diff_df = pd.read_csv("output/diff_report.csv")
                                llm_summary = "❌ LLM is not configured (GEMINI_API_KEY missing or invalid)."
                                if LLM_ENABLED and not diff_df.empty:
                                    llm_summary = query_llm(f"Summarize the following risk changes after a risk event simulation. Focus on top gainers/losers in total risk. Data:\n{diff_df.to_string()}")
                                st.session_state.risk_event_results = {"status": "success", "diff_df": diff_df, "llm_summary": llm_summary, "updated_count": updated_count}
                                st.rerun()
                            else:
                                st.session_state.risk_event_results = {"status": "error", "message": "Simulation did not find any matching relationships to update. Please check your inputs."}
                                st.rerun()
                        except Exception as e:
                            st.session_state.risk_event_results = {"status": "error", "message": f"Error during risk event scenario: {e}"}
                            st.rerun()
            else:
                st.warning("Please provide a Risk Factor Name and a target.")

        if st.session_state.get('risk_event_results') and st.session_state.risk_event_results["status"] == "success":
            results = st.session_state.risk_event_results
            st.success(f"Risk event simulated successfully! Updated {results['updated_count']} risk exposures.")
            diff_df = results["diff_df"]
            if not diff_df.empty:
                st.subheader("📈 Risk Changes After Event")
                st.dataframe(diff_df)
                st.markdown("**LLM Summary:**")
                st.markdown(results["llm_summary"])
                st.download_button("📥 Download Risk Changes CSV", diff_df.to_csv(index=False).encode('utf-8'), file_name="risk_event_risk_changes.csv")
            else:
                st.info("No significant risk changes detected after event. This may be due to the target not being exposed to the specified risk factor, or the multiplier causing too small a change.")
        elif st.session_state.get('risk_event_results') and st.session_state.risk_event_results["status"] == "error":
            st.error(st.session_state.risk_event_results["message"])

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
The schema is for a company risk knowledge graph with the following structure and business logic:
- Node Labels: `Company`, `Blockholder`, `RiskFactor`.
- Relationship Types: `OWNS`, `EXPOSED_TO`.
- **Nodes**:
  - `Company` nodes have properties: `id`, `name`, `sector`, `location`, `total_risk`, `direct_risk`, `dollarized_risk`, `market_cap`.
  - `Blockholder` nodes have properties: `id`, `name`, `type`, `total_risk`, `dollarized_risk`.
  - `RiskFactor` nodes have a `name` property.
- **Relationships**:
  - `(p:Blockholder)-[o:OWNS]->(c:Company)`: `p` owns `c`. The relationship has a `percent` property (e.g., 0.5 for 50%).
  - `(c:Company)-[e:EXPOSED_TO]->(rf:RiskFactor)`: `c` is exposed to `rf`. The relationship has a `weight` property (0.0-1.0).
- **Business Logic & Data Best Practices**:
  - A `Company`'s `direct_risk` is the sum of all `e.weight` values from its outgoing `EXPOSED_TO` relationships.
  - A `Blockholder`'s `total_risk` is a sum of its own `direct_risk` (if it were a company) plus the risks inherited from the companies it owns. The inherited risk is calculated as the owned company's `total_risk` multiplied by the ownership `o.percent`.
  - `dollarized_risk` is calculated by multiplying `total_risk` by `market_cap`.
  - To make queries more robust and handle case sensitivity, use `toLower()` for string matching, for example: `WHERE toLower(c.location) = 'new york'`.
  - To prevent errors with null values, use `coalesce(property, 0)`.
- **Example Query and Answer Pattern**:
  Question: "What is the total risk of Apple Inc.?"
  Correct Query:
  MATCH (c:Company {name: "Apple Inc."})
  RETURN c.total_risk AS TotalRisk
  Question: "Which companies in the Energy sector have a total risk higher than 0.8?"
  Correct Query:
  MATCH (c:Company)
  WHERE c.sector = "Energy" AND coalesce(c.total_risk, 0) > 0.8
  RETURN c.name AS Company, c.total_risk AS TotalRisk
  Question: "List the top 5 riskiest companies."
  Correct Query:
  MATCH (c:Company)
  RETURN c.name AS Company, c.total_risk AS TotalRisk
  ORDER BY TotalRisk DESC
  LIMIT 5
  Question: "Does Berkshire Hathaway Inc. own Exxon Mobil?"
  Correct Query:
  MATCH (b:Blockholder {name: "Berkshire Hathaway Inc."})-[o:OWNS]->(c:Company {name: "Exxon Mobil"})
  RETURN o.percent AS OwnershipPercent
  Question: "Which risk factor is J.P. Morgan Exchange-Traded Fund Trust most affected by?"
  Correct Query:
  MATCH (bh:Blockholder {name: "J.P. Morgan Exchange-Traded Fund Trust"})-[o:OWNS]->(c:Company)-[e:EXPOSED_TO]->(rf:RiskFactor)
  RETURN rf.name AS RiskFactor, sum(coalesce(o.percent, 0) * coalesce(e.weight, 0)) AS InheritedRisk
  ORDER BY InheritedRisk DESC
  LIMIT 1
  Question: "What is the dollarized risk of Apple Inc.?"
  Correct Query:
  MATCH (c:Company {name: "Apple Inc."})
  RETURN c.dollarized_risk AS DollarizedRisk
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
                        st.warning("LLM model could not be initialized. Check API Key.")
                except Exception as e:
                    st.error(f"❌ Error running LLM-generated query or processing results: {e}")
                    st.info("Possible issues: Invalid Cypher from LLM, Memgraph connection, or data error.")

st.markdown("---")
st.caption("Developed for portfolio graph analytics for MPHASIS.AI using Memgraph, Streamlit, and Google Gemini.")