import pandas as pd
from pyvis.network import Network
from gqlalchemy import Memgraph, Node, Relationship
import math
import re
from modules.db_loader import OWNS, EXPOSED_TO

def render_graph_as_html(cypher_query: str) -> str:
    """
    Renders an interactive Pyvis graph as an HTML string based on a Cypher query.
    """
    db = Memgraph()
    results = []
    try:
        results_cursor = db.execute_and_fetch(cypher_query)
        results = list(results_cursor)
    except Exception as e:
        return f"<h1>Error rendering graph. Check your query and database connection.</h1><p>Error: {e}</p>"

    if not results:
        return "<h1>No graph data found. Adjust your filters or check your data.</h1>"

    net = Network(height="1200px", width="100%", notebook=False, cdn_resources='remote', directed=True)

    net.set_options("""
    {
      "layout": {
        "improvedLayout": true
      },
      "physics": {
        "enabled": true,
        "stabilization": {
          "enabled": true,
          "iterations": 1000
        },
        "barnesHut": {
          "gravitationalConstant": -30000,
          "centralGravity": 0.3,
          "springLength": 300,
          "springConstant": 0.05,
          "damping": 0.9,
          "avoidOverlap": 2.5
        },
        "minVelocity": 0.75,
        "solver": "barnesHut"
      },
      "interaction": {
        "dragNodes": true,
        "dragView": true,
        "hover": true,
        "tooltipDelay": 200
      },
      "edges": {
        "font": { "size": 16, "align": "top" },
        "smooth": false,
        "color": { "color": "#95a5a6" },
        "labelHighlightBold": false
      },
      "nodes": {
        "font": { "size": 18, "color": "#343434" },
        "labelHighlightBold": true
      }
    }
    """)

    seen_nodes = set()
    all_dollarized_risks = [getattr(node, 'dollarized_risk', 0) for record in results for node in (record.get('c'), record.get('bh'), record.get('owned_c'), record.get('sub_owned_c')) if node and getattr(node, 'dollarized_risk', 0) is not None]
    max_risk = max(all_dollarized_risks) if all_dollarized_risks else 1.0

    # Get the selected node ID from the query to highlight it
    match = re.search(r"\'(C_\d+|B_\d+)\'", cypher_query)
    selected_node_id = match.group(1) if match else None

    def get_risk_category(risk, max_risk):
        if risk == 0:
            return "No Risk"
        ratio = risk / max_risk
        if ratio <= 0.33:
            return "Low Risk"
        if ratio <= 0.66:
            return "Medium Risk"
        return "High Risk"

    def get_risk_color(risk, max_risk):
        if risk == 0:
            return "#7f8c8d"  # Gray
        ratio = risk / max_risk
        if ratio <= 0.33:
            return "#27ae60"  # Green
        if ratio <= 0.66:
            return "#f1c40f"  # Yellow
        return "#e67e22"  # Orange

    def format_dollars(value):
        if value >= 1_000_000_000:
            return f"${value/1_000_000_000:,.2f}B"
        elif value >= 1_000_000:
            return f"${value/1_000_000:,.2f}M"
        else:
            return f"${value:,.2f}"

    for record in results:
        nodes_in_record = []
        edges_in_record = []
        
        for key, value in record.items():
            if isinstance(value, Node):
                nodes_in_record.append(value)
            elif isinstance(value, Relationship):
                pass
        
        bh_node = record.get('bh')
        c_node = record.get('c')
        owned_c_node = record.get('owned_c')
        sub_owned_c_node = record.get('sub_owned_c')
        rf1_node = record.get('rf1')
        rf2_node = record.get('rf2')
        rf3_node = record.get('rf3')
        
        o_rel = record.get('o')
        o2_rel = record.get('o2')
        o3_rel = record.get('o3')
        e1_rel = record.get('e1')
        e2_rel = record.get('e2')
        e3_rel = record.get('e3')
        
        if c_node:
            nodes_in_record.extend([c_node, owned_c_node, sub_owned_c_node, rf1_node, rf2_node, rf3_node, bh_node])
            edges_in_record.extend([
                (bh_node, c_node, o_rel),
                (c_node, owned_c_node, o2_rel),
                (owned_c_node, sub_owned_c_node, o3_rel),
                (c_node, rf1_node, e1_rel),
                (owned_c_node, rf2_node, e2_rel),
                (sub_owned_c_node, rf3_node, e3_rel)
            ])
            
        bh_node_2 = record.get('bh')
        c1_node = record.get('c1')
        c2_node = record.get('c2')
        c3_node = record.get('c3')
        rf1_node_2 = record.get('rf1')
        rf2_node_2 = record.get('rf2')
        rf3_node_2 = record.get('rf3')
        
        o1_rel = record.get('o1')
        o2_rel_2 = record.get('o2')
        o3_rel_2 = record.get('o3')
        e1_rel_2 = record.get('e1')
        e2_rel_2 = record.get('e2')
        e3_rel_2 = record.get('e3')

        if bh_node_2:
            nodes_in_record.extend([bh_node_2, c1_node, c2_node, c3_node, rf1_node_2, rf2_node_2, rf3_node_2])
            edges_in_record.extend([
                (bh_node_2, c1_node, o1_rel),
                (c1_node, c2_node, o2_rel_2),
                (c2_node, c3_node, o3_rel_2),
                (c1_node, rf1_node_2, e1_rel_2),
                (c2_node, rf2_node_2, e2_rel_2),
                (c3_node, rf3_node_2, e3_rel_2)
            ])
        
        all_exposed_to_weights = [getattr(relationship, 'weight', 0) for _, _, relationship in edges_in_record if relationship and isinstance(relationship, EXPOSED_TO)]
        max_exposure_weight = max(all_exposed_to_weights) if all_exposed_to_weights else 1.0

        for node in [n for n in nodes_in_record if n is not None]:
            node_id = getattr(node, 'id', None) or getattr(node, 'name', None)
            if node_id is None or node_id in seen_nodes:
                continue

            name = getattr(node, "name", node_id)
            node_labels = getattr(node, "labels", [])
            dollarized_risk = getattr(node, 'dollarized_risk', 0)
            
            # --- Determine node shape and size based on type and properties ---
            node_shape = 'dot'
            if "RiskFactor" in node_labels:
                node_shape = 'diamond'
            
            node_size = 10
            border_width = 1
            border_color = "#BB86FC"

            if node_id == selected_node_id:
                node_color = "#59565D"
                node_size = 50
                border_width = 3
            elif "RiskFactor" in node_labels:
                node_exposure_weight = sum(getattr(relationship, 'weight', 0) for _, target, relationship in edges_in_record if target and getattr(target, 'name', None) == name and isinstance(relationship, EXPOSED_TO))
                if node_exposure_weight and max_exposure_weight > 0:
                    node_size = 10 + 20 * (node_exposure_weight / max_exposure_weight)
                node_color = "#e74c3c"
            elif dollarized_risk and max_risk > 0:
                node_size = 10 + 20 * (dollarized_risk / max_risk)
                node_color = get_risk_color(dollarized_risk, max_risk)
            else:
                node_color = "#7f8c8d"
            # -------------------------------------------------------------------
            
            risk_category = get_risk_category(dollarized_risk, max_risk)
            formatted_dollar_risk = format_dollars(dollarized_risk)
            node_title = f"{name}<br>Risk Category: {risk_category}<br>Risk: {formatted_dollar_risk}"
                
            net.add_node(
                node_id,
                label=name,
                title=node_title,
                color=node_color,
                size=max(node_size, 10),
                shape=node_shape,
                border_width=border_width,
                border_color=border_color
            )
            seen_nodes.add(node_id)
        
        for source, target, relationship in [e for e in edges_in_record if e[0] and e[1] and e[2]]:
            node_id_src = getattr(source, 'id', None) or getattr(source, 'name', None)
            node_id_tgt = getattr(target, 'id', None) or getattr(target, 'name', None)
            
            rel_type = "UNKNOWN"
            edge_label = ""
            
            if isinstance(relationship, OWNS):
                rel_type = "OWNS"
                percent = getattr(relationship, "percent", None)
                if percent is not None:
                    edge_label = f"OWNS: {percent:.1%}"
                    
            elif isinstance(relationship, EXPOSED_TO):
                rel_type = "EXPOSED_TO"
                weight = getattr(relationship, "weight", None)
                if weight is not None:
                    edge_label = f"EXPOSURE_OF: {weight:.1%}"
            
            if node_id_src and node_id_tgt:
                net.add_edge(
                    node_id_src,
                    node_id_tgt,
                    title=edge_label,
                    label=edge_label,
                    color="#95a5a6"
                )
    
    html = net.generate_html()
    
    return html