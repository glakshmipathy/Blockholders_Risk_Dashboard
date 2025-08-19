import os
import csv
import json
import datetime
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()
from modules.db_loader import Blockholder, Company, RiskFactor, OWNS, EXPOSED_TO

class RiskEngine:
    """
    Manages risk propagation, calculation of derived metrics, snapshotting,
    and scenario simulations.
    """
    def __init__(self, uri=None, user=None, password=None):
        self.uri = uri or os.getenv("MEMGRAPH_URI")
        self.user = user or os.getenv("MEMGRAPH_USER")
        self.password = password or os.getenv("MEMGRAPH_PASSWORD")

        if not all([self.uri, self.user, self.password is not None]):
            raise ValueError("Memgraph connection details (URI, USER, PASSWORD) are required in .env file.")

        try:
            self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))
            self.driver.verify_connectivity()
            print(f"INFO: RiskEngine connected to Memgraph at {self.uri}.")
        except Exception as e:
            print(f"ERROR: RiskEngine failed to connect to Memgraph at {self.uri}. Ensure Memgraph is running. Error: {e}")
            raise

    def close(self):
        if self.driver:
            self.driver.close()
            print("INFO: RiskEngine connection closed.")

    def compute_total_risk(self, max_iterations=15):
        """
        Computes total_risk for all companies/blockholders by propagating direct risks through
        ownership chains. 'indirect_risk' property is removed for simplicity with current model.
        """
        print("\n--- Starting Total Risk Propagation ---")
        with self.driver.session() as session:
            session.run("""
                MATCH (n) WHERE n:Company OR n:Blockholder
                SET n.total_risk = 0.0,
                    n.direct_risk = coalesce(n.direct_risk, 0.0)
                REMOVE n.indirect_risk
            """)
            print("INFO: Resetting previous risk properties and removed indirect_risk.")

            session.run("""
                MATCH (c:Company)-[e:EXPOSED_TO]->(r:RiskFactor)
                WITH c, sum(toFloat(e.weight)) AS direct_risk_sum_raw
                SET c.direct_risk = direct_risk_sum_raw,
                    c.total_risk = direct_risk_sum_raw
            """)
            print("INFO: Initial direct risks assigned to Companies.")

            session.run("""
                MATCH (b:Blockholder)
                WHERE b.total_risk IS NULL
                SET b.total_risk = 0.0
            """)
            print("INFO: Initialized Blockholder total_risk to 0 where not set.")

            updated = True
            iteration = 0

            while updated and iteration < max_iterations:
                updates_count = session.write_transaction(self._propagate_risk_step)
                print(f"INFO: Iteration {iteration + 1}: Nodes updated = {updates_count}")
                updated = updates_count > 0
                iteration += 1

            if iteration >= max_iterations and updated:
                print(f"WARNING: Risk propagation reached max iterations ({max_iterations}) and may not have fully converged. Consider increasing max_iterations.")
            else:
                print(f"INFO: Risk propagation converged in {iteration} iterations.")
        print("--- Total Risk Propagation Complete ---")

    def normalize_risk_scores(self, new_property_name="normalized_risk", max_score=100.0):
        """
        Normalizes total_risk scores across the graph to a new scale (e.g., 0-100).
        Sets the result on a new property (e.g., 'normalized_risk').
        """
        print(f"\n--- Starting Risk Score Normalization for '{new_property_name}' ---")
        with self.driver.session() as session:
            max_risk_result = session.read_transaction(lambda tx: tx.run("""
                MATCH (n) WHERE n.total_risk IS NOT NULL
                RETURN max(n.total_risk) AS max_total_risk
            """).single())
            
            max_total_risk = max_risk_result["max_total_risk"] if max_risk_result and max_risk_result["max_total_risk"] > 0 else 1.0
            
            updated_nodes_count = session.write_transaction(lambda tx: tx.run(f"""
                MATCH (n) WHERE n.total_risk IS NOT NULL
                SET n.{new_property_name} = (n.total_risk / {max_total_risk}) * {max_score}
                RETURN count(n) AS updatedCount
            """).single()["updatedCount"])
        
        print(f"INFO: Normalized risk scores for {updated_nodes_count} nodes. Max original risk was {max_total_risk:.2f}.")
        print("--- Finished Risk Score Normalization ---")

    def dollarize_risk(self):
        """
        Calculates dollarized risk for all companies, blockholders, and risk factors
        and stores the result in a new property.
        """
        print("\n--- Starting Dollarized Risk Calculation ---")
        with self.driver.session() as session:
            # Dollarize risk for companies
            session.run("""
                MATCH (c:Company)
                SET c.dollarized_risk = coalesce(c.total_risk, 0) * coalesce(c.market_cap, 0)
            """)

            # Dollarize risk for blockholders (by summing the dollarized risk of what they own)
            session.run("""
                MATCH (bh:Blockholder)-[o:OWNS]->(c:Company)
                WITH bh, sum(coalesce(o.percent, 0) * coalesce(c.dollarized_risk, 0)) AS total_inherited_dollar_risk
                SET bh.dollarized_risk = total_inherited_dollar_risk
            """)
            
            # Dollarize risk for risk factors (by summing the dollarized risk of all companies exposed to it)
            session.run("""
                MATCH (c:Company)-[e:EXPOSED_TO]->(rf:RiskFactor)
                WITH rf, sum(coalesce(c.dollarized_risk, 0) * coalesce(e.weight, 0)) AS total_dollar_exposure
                SET rf.dollarized_risk = total_dollar_exposure
            """)
        print("--- Dollarized Risk Calculation Complete ---")

    def _propagate_risk_step(self, tx):
        """
        Single step of iterative risk propagation. Updates total_risk for owning nodes.
        """
        query = """
            MATCH (p)-[o:OWNS]->(c:Company)
            WHERE c.total_risk IS NOT NULL
              AND (p:Company OR p:Blockholder)
            WITH p, toFloat(coalesce(p.direct_risk, 0)) AS current_p_direct_risk,
                 sum(toFloat(o.percent) * toFloat(coalesce(c.total_risk, 0))) AS total_inherited_from_owned
            WITH p, current_p_direct_risk, total_inherited_from_owned,
                 (current_p_direct_risk + total_inherited_from_owned) AS new_p_total_risk
            WHERE abs(toFloat(coalesce(p.total_risk, 0)) - new_p_total_risk) > 0.0001
            SET p.total_risk = new_p_total_risk
            RETURN count(p) AS updates
        """
        updates_count = tx.run(query).single()["updates"]
        return updates_count

    def compute_sector_concentration(self, threshold=0.3):
        """Identifies sectors with high concentration of dollarized risk."""
        print("\n--- Computing Sectoral Concentration Risk (Dollarized) ---")
        with self.driver.session() as session:
            sector_results = session.read_transaction(
                lambda tx: tx.run("""
                    MATCH (c:Company)
                    WHERE c.sector IS NOT NULL AND c.dollarized_risk IS NOT NULL
                    RETURN c.sector AS sector, sum(coalesce(c.dollarized_risk, 0)) AS sector_risk
                """).data()
            )
            sector_risks_data = []
            total_portfolio_risk = 0.0
            for row in sector_results:
                sector_risks_data.append({"sector": row["sector"], "sector_risk": row["sector_risk"]})
                total_portfolio_risk += row["sector_risk"]
            overexposed = []
            for s in sector_risks_data:
                ratio = s["sector_risk"] / total_portfolio_risk if total_portfolio_risk else 0
                if ratio > threshold:
                    overexposed.append({"sector": s["sector"], "share_pct": round(ratio * 100, 2)})
            print("--- Finished Computing Sectoral Concentration Risk ---")
            return overexposed

    def get_critical_nodes_by_degree(self, top_n=10):
        """Identifies top N critical companies/blockholders based on network degree."""
        print("\n--- Computing Critical Companies by Network Degree ---")
        with self.driver.session() as session:
            critical_nodes = session.read_transaction(
                lambda tx: tx.run("""
                    MATCH (n)
                    WHERE n:Company OR n:Blockholder
                    MATCH (n)-[r]-()
                    RETURN n.name AS name, count(r) AS degree
                    ORDER BY degree DESC
                    LIMIT $n
                """, n=top_n).data()
            )
        print("--- Finished Computing Critical Companies by Network Degree ---")
        return critical_nodes

    # The compute_pagerank_centrality function has been removed.
    
    def export_risks_to_csv(self, filename="output/risk_scores.csv"):
        """Exports current graph state of companies/blockholders with their dollarized risk scores to a CSV file."""
        print(f"--- Exporting dollarized risk scores to {filename} ---")
        os.makedirs("output", exist_ok=True)
        with self.driver.session() as session:
            result = session.run("""
                MATCH (n) WHERE n:Company OR n:Blockholder
                RETURN n.name AS name,
                        coalesce(n.dollarized_risk, 0) AS dollarized_risk
                ORDER BY dollarized_risk DESC
            """)
            with open(filename, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["Node Name", "Dollarized Risk"])
                for row in result:
                    writer.writerow([
                        row["name"],
                        round(row["dollarized_risk"], 2)
                    ])
        print("--- Risk scores exported. ---")

    def export_snapshot(self, filepath):
        """Exports current graph state of companies to a JSON file."""
        print(f"--- Exporting snapshot to {filepath} ---")
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with self.driver.session() as session:
            result = session.run("""
                MATCH (c:Company)
                RETURN c.id AS id, c.name AS name,
                        coalesce(c.direct_risk, 0) AS direct_risk,
                        coalesce(c.total_risk, 0) AS total_risk,
                        coalesce(c.dollarized_risk, 0) AS dollarized_risk,
                        coalesce(c.sector, "N/A") AS sector,
                        coalesce(c.location, "N/A") AS location
            """).data()
            with open(filepath, "w") as f:
                json.dump(result, f, indent=2)
        print("--- Snapshot exported. ---")

    def generate_diff(self, before_path, after_path, output="output/diff_report.csv"):
        """Compares two snapshots and generates a CSV report of risk deltas."""
        print("\n--- Generating Diff Report ---")
        try:
            if not os.path.exists(before_path) or os.path.getsize(before_path) == 0:
                print(f"ERROR: Before snapshot not found or is empty: {before_path}")
                return
            if not os.path.exists(after_path) or os.path.getsize(after_path) == 0:
                print(f"ERROR: After snapshot not found or is empty: {after_path}")
                return

            with open(before_path) as f1, open(after_path) as f2:
                before = {c["id"]: c for c in json.load(f1)}
                after = {c["id"]: c for c in json.load(f2)}

            diff = []
            for cid, after_data in after.items():
                before_data = before.get(cid, {})
                delta = (after_data.get("dollarized_risk", 0) or 0) - (before_data.get("dollarized_risk", 0) or 0)
                if abs(delta) > 0.01:
                    diff.append({
                        "id": cid,
                        "name": after_data.get("name", "N/A"),
                        "risk_before": round(before_data.get("dollarized_risk", 0) or 0, 2),
                        "risk_after": round(after_data.get("dollarized_risk", 0) or 0, 2),
                        "delta": round(delta, 2),
                        "sector": after_data.get("sector", "N/A"),
                        "location": after_data.get("location", "N/A")
                    })
            
            os.makedirs(os.path.dirname(output), exist_ok=True)
            with open(output, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=["id", "name", "risk_before", "risk_after", "delta", "sector", "location"])
                writer.writeheader()
                writer.writerows(diff)
            print(f"--- Diff report generated: {output} ---")
        except Exception as e:
            print(f"FATAL ERROR: An unexpected error occurred during diff generation: {e}")

    def simulate_acquisition(self, acquiring_company_id: str, acquired_company_id: str, ownership_percent: float) -> bool:
        """Simulates an acquisition."""
        print(f"\n--- Simulating Acquisition: {acquiring_company_id} acquires {acquired_company_id} with {ownership_percent:.2%} ownership ---")
        with self.driver.session() as session:
            try:
                check_query = """
                MATCH (acquirer:Company {id: $acquiring_company_id})
                MATCH (acquired:Company {id: $acquired_company_id})
                RETURN acquirer, acquired
                """
                check_result = session.run(check_query, acquiring_company_id=acquiring_company_id, acquired_company_id=acquired_company_id).single()
                if not check_result:
                    print("ERROR: One or both companies not found for acquisition simulation.")
                    return False
                session.run(f"""
                    MATCH (p)-[o:OWNS]->(c:Company {{id: '{acquired_company_id}'}})
                    DELETE o
                """)
                print(f"INFO: Removed existing ownerships for acquired company {acquired_company_id}.")
                current_year = datetime.datetime.now().year
                create_owns_query = """
                MATCH (acquirer:Company {id: $acquiring_company_id})
                MATCH (acquired:Company {id: $acquired_company_id})
                MERGE (acquirer)-[o:OWNS]->(acquired)
                SET o.percent = $ownership_percent,
                    o.year = $current_year
                """
                session.run(create_owns_query, acquiring_company_id=acquiring_company_id, acquired_company_id=acquired_company_id, ownership_percent=ownership_percent, current_year=current_year)
                print(f"INFO: Created OWNS relationship: {acquiring_company_id} OWNS {acquired_company_id} ({ownership_percent:.2%}).")
                session.run(f"""
                    MATCH (c:Company {{id: '{acquired_company_id}'}})
                    SET c.role = 'acquired'
                """)
                print(f"INFO: Set role of {acquired_company_id} to 'acquired'.")
                return True
            except Exception as e:
                print(f"ERROR: Failed to simulate acquisition: {e}")
                return False

    def simulate_divestiture(self, divesting_company_id: str, divested_company_id: str) -> bool:
        """Simulates a divestiture."""
        print(f"\n--- Simulating Divestiture: {divesting_company_id} divests {divested_company_id} ---")
        with self.driver.session() as session:
            try:
                delete_owns_query = """
                MATCH (d:Company {id: $divesting_company_id})-[o:OWNS]->(t:Company {id: $divested_company_id})
                DELETE o
                """
                result = session.run(delete_owns_query, divesting_company_id=divesting_company_id, divested_company_id=divested_company_id)
                if result.summary.counters.relationships_deleted > 0:
                    print(f"INFO: Deleted OWNS relationship: {divesting_company_id} no longer owns {divested_company_id}.")
                    check_ownership_query = f"""
                        MATCH (p:Company)-[:OWNS]->(c:Company {{id: '{divested_company_id}'}})
                        RETURN count(p) AS owners_count
                    """
                    owners_count = session.run(check_ownership_query).single()["owners_count"]
                    if owners_count == 0:
                        session.run(f"""
                            MATCH (c:Company {{id: '{divested_company_id}'}})
                            SET c.role = 'company'
                        """)
                        print(f"INFO: Reset role of {divested_company_id} to 'company' as it has no more direct owners.")
                    return True
                else:
                    print(f"WARNING: No OWNS relationship found between {divesting_company_id} and {divested_company_id} to divest.")
                    return False
            except Exception as e:
                print(f"ERROR: Failed to simulate divestiture: {e}")
                return False

    def simulate_risk_event(self, risk_factor_name: str, impact_multiplier: float, target_company_id: str = None, target_sector: str = None, target_location: str = None) -> int:
        """
        Simulates a risk event with optional targeting to a specific company, sector, or location.
        Returns the count of updated exposures.
        """
        print(f"\n--- Simulating Risk Event: '{risk_factor_name}' with impact {impact_multiplier} ---")
        with self.driver.session() as session:
            try:
                match_clause = "MATCH (c:Company)-[e:EXPOSED_TO]->(r:RiskFactor {name: $risk_factor_name})"
                where_clauses = []
                params = {"risk_factor_name": risk_factor_name, "impact_multiplier": impact_multiplier}

                if target_company_id:
                    where_clauses.append("c.id = $target_company_id")
                    params["target_company_id"] = target_company_id
                if target_sector:
                    where_clauses.append("c.sector = $target_sector")
                    params["target_sector"] = target_sector
                if target_location:
                    where_clauses.append("c.location = $target_location")
                    params["target_location"] = target_location
                
                if where_clauses:
                    match_clause += " WHERE " + " AND ".join(where_clauses)
                
                update_query = f"""
                {match_clause}
                SET e.weight = CASE
                                        WHEN e.weight * $impact_multiplier > 1.0 THEN 1.0
                                        WHEN e.weight * $impact_multiplier < 0.0 THEN 0.0
                                        ELSE e.weight * $impact_multiplier
                                        END
                RETURN count(e) AS updated_exposures
                """
                
                print(f"INFO: Running Cypher Query: {update_query} with params: {params}")
                
                result = session.write_transaction(lambda tx: tx.run(update_query, **params).single())
                updated_count = result["updated_exposures"] if result else 0
                print(f"INFO: Updated {updated_count} '{risk_factor_name}' risk exposures.")
                return updated_count
            except Exception as e:
                print(f"ERROR: Failed to simulate risk event: {e}")
                return 0
