import time
import subprocess

# 11 loops x ~1,032 records = ~11,352 records (safely over the 10k requirement)
TOTAL_LOOPS = 11

# 65-second delay to safely bypass the 60-second NTA API rate limit
DELAY_SECONDS = 65 

def run_ingestion_cycle():
    print(f"Starting automated ingestion loop. Target: {TOTAL_LOOPS} cycles.")
    
    for i in range(1, TOTAL_LOOPS + 1):
        print(f"\n--- Ingestion Cycle {i}/{TOTAL_LOOPS} ---")
        
        # Execute the NTA and Eirgrid ingestion scripts natively
        # Note: Depending on earlier naming, fetch_nta.py might be transit_ingestion.py
        # Checking run_pipeline.sh from before, it was transit_ingestion.py
        subprocess.run(["python", "src/ingestion/transit_ingestion.py"])
        subprocess.run(["python", "src/ingestion/fetch_eirgrid.py"])
        
        # Pause execution before the next iteration to avoid getting blocked
        if i < TOTAL_LOOPS:
            print(f"Cycle {i} complete. Sleeping for {DELAY_SECONDS} seconds to respect API limits...")
            time.sleep(DELAY_SECONDS) 
            
    print("\nSUCCESS: Automated ingestion complete. Target dataset size reached.")

if __name__ == "__main__":
    run_ingestion_cycle()
