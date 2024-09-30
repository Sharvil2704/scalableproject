import sqlite3

def balance_algorithm(query_stream):
    conn = sqlite3.connect('advertisers.db')
    cursor = conn.cursor()

    # Initialize hashmaps to store initial budgets and spending limits
    initial_budgets = {}
    spending_limits = {}

    # Retrieve all advertisers and their initial budgets
    cursor.execute("SELECT advertiser, budget FROM advertisers")
    advertisers = cursor.fetchall()

    for adv in advertisers:
        advertiser_name = adv[0]
        initial_budgets[advertiser_name] = adv[1]  # Store the initial budget
        spending_limits[advertiser_name] = adv[1] * 0.5  # Set the initial spending limit to 50%

    transactions = []
    serial_no = 1

    # Start a single transaction for the entire processing
    conn.execute("BEGIN TRANSACTION")

    try:
        for char in query_stream:
            cursor.execute("SELECT * FROM advertisers")
            advertisers = cursor.fetchall()

            eligible_advertisers = []

            for adv in advertisers:
                advertiser_name = adv[0]
                categories = adv[1].split(',')
                current_budget = adv[2]

                if char in categories and current_budget > 0:
                    # Check if the advertiser is within their current spending limit
                    current_spending_limit = spending_limits[advertiser_name]

                    if current_budget > current_spending_limit:
                        # Advertiser is eligible for bidding
                        eligible_advertisers.append((advertiser_name, current_budget))

            if eligible_advertisers:
                # Select the advertiser with the highest remaining budget
                winner = max(eligible_advertisers, key=lambda x: x[1])

                if winner[1] > 0:
                    advertiser_name = winner[0]

                    # Decrement the budget of the selected advertiser by 1
                    cursor.execute("UPDATE advertisers SET budget = budget - 1 WHERE advertiser = ?", (advertiser_name,))
                    
                    # After spending, check if the advertiser has reached their limit
                    current_budget = winner[1] - 1
                    if current_budget <= spending_limits[advertiser_name]:
                        # Halve their spending limit for the next round
                        spending_limits[advertiser_name] /= 2

                    # Record the transaction
                    transactions.append((serial_no, advertiser_name))
                    serial_no += 1

        # Commit the transaction if everything goes well
        conn.commit()

    except Exception as e:
        # Rollback in case of any error
        conn.rollback()
        print(f"An error occurred: {e}")

    finally:
        conn.close()

    return transactions


# Read query stream from file
with open('query_stream.txt', 'r') as f:
    query_stream = f.read()

# Run the balance algorithm
transactions = balance_algorithm(query_stream)

# Save the transactions to a file
with open('transactions.txt', 'w') as f:
    for transaction in transactions:
        f.write(f"{transaction[0]},{transaction[1]}\n")
