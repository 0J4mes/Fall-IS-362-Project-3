# main.py
from chinook_python_centric import ChinookAnalyzer
from chinook_simple_sql import simple_sql_solution
from setup_database import setup_database
import os


def main():
    """Main analysis runner"""
    print("=" * 60)
    print("IS 362 - Project 3: Chinook Database Analysis")
    print("=" * 60)

    # Step 1: Ensure database is setup
    print("\n1. Checking database setup...")
    db_path = setup_database()

    if not db_path:
        print("Cannot proceed without database. Exiting.")
        return

    print(f"Using database: {db_path}")

    # Step 2: Ask user which method to use
    print("\n2. Choose analysis method:")
    print("   1. Python-centric approach (step-by-step merging)")
    print("   2. Simple SQL approach (single query)")
    print("   3. Both methods (comparison)")

    try:
        choice = input("\nEnter your choice (1-3): ").strip()

        if choice == '1':
            print("\n" + "=" * 50)
            print("PYTHON-CENTRIC APPROACH")
            print("=" * 50)
            analyzer = ChinookAnalyzer(db_path)
            result = analyzer.run_complete_analysis()

        elif choice == '2':
            print("\n" + "=" * 50)
            print("SIMPLE SQL APPROACH")
            print("=" * 50)
            result = simple_sql_solution()

        elif choice == '3':
            print("\n" + "=" * 50)
            print("COMPARISON ANALYSIS")
            print("=" * 50)

            # Python approach
            print("\n--- Python Approach ---")
            analyzer = ChinookAnalyzer(db_path)
            python_result = analyzer.run_complete_analysis()

            # SQL approach
            print("\n--- SQL Approach ---")
            sql_result = simple_sql_solution()

            if python_result is not None and sql_result is not None:
                print("\n" + "=" * 30)
                print("COMPARISON RESULTS")
                print("=" * 30)
                print(f"Python rows: {len(python_result)}")
                print(f"SQL rows:    {len(sql_result)}")

                if len(python_result) == len(sql_result):
                    print("Both methods produced identical row counts")
                else:
                    print("Different row counts between methods")

        else:
            print("Invalid choice. Please run again and choose 1, 2, or 3.")
            return

        print("\nAnalysis complete!")

    except KeyboardInterrupt:
        print("\n\nAnalysis interrupted by user.")
    except Exception as e:
        print(f"\nError during analysis: {e}")


if __name__ == "__main__":
    main()