# main.py (temporary test)
from engine import MiniSQLEngine

if __name__ == "__main__":
    engine = MiniSQLEngine()
    engine.load_table("data/customers.csv")
    engine.load_table("data/products.csv")
    
    # Print first row of customers to verify
    print("\nFirst customer:", engine.tables["customers"][0])