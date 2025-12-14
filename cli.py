# cli.py
from engine import MiniSQLEngine
import sys

def print_results(results):
    if not results:
        print("Empty result set.")
        return
    
    if len(results) == 1 and "COUNT" in results[0]:
        print(results[0]["COUNT"])
        return
    
    # Pretty print table
    headers = results[0].keys()
    print(" | ".join(str(h) for h in headers))
    print("-" * 50)
    for row in results:
        print(" | ".join(str(row.get(h, "")) for h in headers))

def main():
    print("Mini SQL Engine")
    print("Type 'load <csv_path>' to load a table")
    print("Type 'exit' or 'quit' to exit")
    print()
    
    engine = MiniSQLEngine()
    
    while True:
        try:
            query = input("sql> ").strip()
            if query.lower() in ["exit", "quit"]:
                print("Goodbye!")
                break
            if not query:
                continue
            
            if query.lower().startswith("load "):
                path = query[5:].strip().strip('"\'')
                engine.load_table(path)
                continue
            
            results = engine.execute(query)
            print_results(results)
            print()
            
        except Exception as e:
            print(f"Error: {e}")
            print()

if __name__ == "__main__":
    main()