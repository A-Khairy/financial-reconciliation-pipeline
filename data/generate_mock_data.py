import csv
import random
import uuid
from datetime import datetime, timezone

NUM_RECORDS = 50
currencies = ["EUR", "GBP", "USD"]


def generate_feeds():
    internal_rows = []
    external_rows = []

    for _ in range(NUM_RECORDS):
        tx_id = f"tx_{uuid.uuid4().hex[:12]}"
        account_id = f"acc_{random.randint(1000, 9999)}"
        amount = round(random.uniform(5.0, 500.0), 2)
        curr = random.choice(currencies)
        now_iso = datetime.now(timezone.utc).isoformat()

        internal_rows.append([tx_id, account_id, amount, curr, "SETTLED", now_iso])

        # Anomaly simulation:
        roll = random.random()
        if roll < 0.75:
            # Clean match
            external_rows.append([f"ext_{uuid.uuid4().hex[:10]}", tx_id, amount, curr, "CLEARED", now_iso])
        elif roll < 0.85:
            # Discrepancy: Amount mismatch (fee or rate issue)
            external_rows.append([f"ext_{uuid.uuid4().hex[:10]}", tx_id, round(amount - 2.50, 2), curr, "CLEARED", now_iso])
        elif roll < 0.95:
            # Discrepancy: Missing in gateway
            pass
        else:
            # Discrepancy: External ghost transaction (missing in internal ledger)
            external_rows.append([f"ext_{uuid.uuid4().hex[:10]}", f"orphan_{uuid.uuid4().hex[:8]}", amount, curr, "CLEARED", now_iso])

    with open("data/internal_feed.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["transaction_id", "account_id", "amount", "currency", "status", "created_at"])
        writer.writerows(internal_rows)

    with open("data/external_feed.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["provider_tx_id", "internal_reference_id", "cleared_amount", "currency", "settlement_status", "cleared_at"])
        writer.writerows(external_rows)

    print(f"Generated {len(internal_rows)} internal rows and {len(external_rows)} external gateway rows.")


if __name__ == "__main__":
    generate_feeds()