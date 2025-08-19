import subprocess
import os
import sys

# Get the directory where the script is located, which is the project root.
# This fixes the pathing issue.
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# List of data generation scripts to run
scripts_to_run = [
    os.path.join(project_root, 'scripts', 'generate_cik_ticker_map.py'),
    os.path.join(project_root, 'scripts', 'generate_fema_risk_map.py'),
    os.path.join(project_root, 'scripts', 'generate_market_cap.py')
]

def run_scripts():
    """
    Executes all data generation scripts from the project root.
    """
    print("--- Starting all data generation scripts ---")

    for script in scripts_to_run:
        print(f"Running: {script}")
        try:
            # Now run the script with its full path
            result = subprocess.run([sys.executable, script], check=True, capture_output=True, text=True)
            print(result.stdout)
            if result.stderr:
                print(f"Error in {script}:\n{result.stderr}")
        except FileNotFoundError:
            print(f"ERROR: Python script not found at {script}. Please check the path.")
            return False
        except subprocess.CalledProcessError as e:
            print(f"ERROR: {script} failed with exit code {e.returncode}. Output:\n{e.stdout}\nErrors:\n{e.stderr}")
            return False
            
    print("--- All data generation scripts finished successfully. ---")
    return True

if __name__ == "__main__":
    run_scripts()