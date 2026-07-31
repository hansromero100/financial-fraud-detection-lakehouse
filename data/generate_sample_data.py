import csv
import os
import random

SEED = 42
TRANSACTION_TYPES = ["CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER"]
FRAUD_TYPES = ["CASH_OUT", "TRANSFER"]
NUM_RECORDS = 500
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "sample_paysim.csv")


def generate_sample_data(num_records=NUM_RECORDS):
    random.seed(SEED)
    rows = []
    for i in range(1, num_records + 1):
        is_fraud = 1 if random.random() < 0.05 else 0
        if is_fraud:
            tx_type = random.choice(FRAUD_TYPES)
            amount = round(random.uniform(100000, 10000000), 2)
        else:
            tx_type = random.choice(TRANSACTION_TYPES)
            amount = round(random.uniform(1000, 500000), 2)

        step = random.randint(1, 743)
        name_orig = f"C{random.randint(1000000, 9999999)}"
        name_dest_prefix = "M" if random.random() < 0.3 else "C"
        name_dest = f"{name_dest_prefix}{random.randint(1000000, 9999999)}"
        old_balance_org = round(random.uniform(0, 5000000), 2)
        new_balance_org = round(old_balance_org - amount, 2)
        old_balance_dest = round(random.uniform(0, 5000000), 2)
        new_balance_dest = round(old_balance_dest + amount, 2)
        is_flagged_fraud = 1 if (amount > 200000 and tx_type in FRAUD_TYPES) else 0

        rows.append([
            step, tx_type, amount, name_orig,
            old_balance_org, new_balance_org,
            name_dest, old_balance_dest, new_balance_dest,
            is_fraud, is_flagged_fraud,
        ])

    with open(OUTPUT_PATH, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "step", "type", "amount", "nameOrig",
            "oldbalanceOrg", "newbalanceOrig",
            "nameDest", "oldbalanceDest", "newbalanceDest",
            "isFraud", "isFlaggedFraud",
        ])
        writer.writerows(rows)

    print(f"Generated {num_records} records -> {OUTPUT_PATH}")


if __name__ == "__main__":
    generate_sample_data()
