import os
import uuid
import random
import pandas as pd
from faker import Faker
from datetime import timedelta

# Setup
SEEDS_PATH = './seeds'
os.makedirs(SEEDS_PATH, exist_ok=True)
faker = Faker()
random.seed(42)

# Sales funnel stages
STATUS_DIM = [
    (1, 'Lead'),
    (2, 'Contacted'),
    (3, 'Qualified'),
    (4, 'Proposal Sent'),
    (5, 'Negotiation'),
    (6, 'Closed')
]

def generate_users(n_users: int) -> pd.DataFrame:
    users = []
    for _ in range(n_users):
        user_id = str(uuid.uuid4())
        name = faker.name()
        email = faker.unique.email()
        created_at = faker.date_between(start_date='-2y', end_date='-1y')
        users.append({
            'user_id': user_id,
            'name': name,
            'email': email,
            'created_at': created_at
        })
    return pd.DataFrame(users)

def generate_funnel_movements(users_df: pd.DataFrame, target_rows: int = 10000) -> pd.DataFrame:
    """Generate approximately 500 rows of status transitions."""
    movements = []
    total_rows = 0

    for _, user in users_df.iterrows():
        stages = random.randint(2, 6)
        base_date = pd.to_datetime(user['created_at'])

        for i in range(stages):
            if total_rows >= target_rows:
                break

            status_id = STATUS_DIM[i][0]
            transition_date = (base_date + timedelta(days=i * random.randint(5, 15))).date()

            movements.append({
                'movement_id': str(uuid.uuid4()),
                'user_id': user['user_id'],
                'status_id': status_id,
                'transition_date': transition_date
            })

            total_rows += 1

        if total_rows >= target_rows:
            break

    return pd.DataFrame(movements)

def export_csv(df: pd.DataFrame, filename: str):
    path = os.path.join(SEEDS_PATH, filename)
    df.to_csv(path, index=False)
    print(f"Exported {filename} ({len(df)} rows)")

def main():
    print("Generating 500 fake funnel transitions...")

    # Generate dimension status
    df_status = pd.DataFrame(STATUS_DIM, columns=['status_id', 'status_name'])
    export_csv(df_status, 'dim_status.csv')

    # Generate users
    df_users = generate_users(150)  # Generate more to allow filtering to ~500 rows
    export_csv(df_users, 'dim_users.csv')

    # Generate transitions
    df_movements = generate_funnel_movements(df_users, target_rows=500)
    export_csv(df_movements, 'status_movements.csv')

    print("Data ready in ./seeds for dbt ingestion.")

if __name__ == "__main__":
    main()
