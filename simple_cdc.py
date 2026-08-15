import psycopg2 
import time 
import json 

from datetime import datetime 
import os 

DB_CONFIG = { 
    "host" : os.getenv("PG_HOST"),
    "port" : os.getenv("PG_PORT"),
    "dbname" : os.getenv("PG_DB"),
    "user" : os.getenv("PG_USER"),
    "password" : os.getenv("PG_PASSWORD"),
}

POLL_INTERVAL = 5 # seconds
CHANGE_LOG = "postgres_changes.log"

def setup_database():
    """Sets up the database connection and returns the connection object."""
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True 
    cur = conn.cursor() 

# CREATE THE USER TABLE 
    cur.execute(
        """
        create table if not exists users ( 
        id serial primary key , 
        name text not null, 
        email text, 
        created_at timestamp default current_timestamp,
        updated_at timestamp default current_timestamp
        )
        """
    )

# CREATE TRIGGER TO AUTO_UPDATE UPDATED_AT
    cur.execute (
        """
        create or replace function update_updated_at_column()
        returns trigger as $$
        begin
            new.updated_at = now();
            return new;
        end;
        $$ language 'plpgsql';
        """
    )

# attach trigger to table 
    cur.execute(
        """
        DROP TRIGGER IF EXISTS update_users_updated_at ON users;
        CREATE TRIGGER update_users_updated_at
        BEFORE UPDATE ON users
        FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
        """
    )   

 # Seed initial data (safe to re-run)
    cur.execute(f"""
        INSERT INTO users (id, name, email)
        VALUES (1, 'Alice', 'alice@example.com')
        ON CONFLICT (id) DO NOTHING;
    """)

    cur.close()
    conn.close()
    print(f"Database '{DB_CONFIG['dbname']}' table users ready.")

def get_last_seen_timestamp():
    """Read last processed timestamp from our change log"""
    try:
        with open(CHANGE_LOG, 'r') as f:
            lines = f.readlines()
            if lines:
                last_line = lines[-1].strip()
                if last_line:
                    return json.loads(last_line).get('timestamp')
    except FileNotFoundError:
        pass
    return None

def detect_changes(last_timestamp): 
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

     # Query changed rows (we don't know operation type from timestamp alone)
    cur.execute(f"""
        SELECT id, name, email, updated_at
        FROM users
        WHERE updated_at > %s
        ORDER BY updated_at
    """, (last_timestamp,) if last_timestamp else ('1970-01-01',))

    changes = [dict(id=row[0], name=row[1], email=row[2], updated_at=row[3].isoformat())
               for row in cur.fetchall()]

    cur.close()
    conn.close()
    return changes

def log_change(change) : 
    change['detected_at'] = datetime.now().isoformat()
    with open(CHANGE_LOG, 'a') as f : 
        f.write(json.dumps(change)+ '\n')
    print(f"DETECTED: {change}")

def main() :
    print("Starting simple Postgres CDC demo...")
    print(f"Watching table users for changes (polling every 60s)")
    print(f"Changes logged to: {CHANGE_LOG}")
    print("-" * 60)

    setup_database()

    try:
        while True:
            last_ts = get_last_seen_timestamp()
            changes = detect_changes(last_ts)

            for change in changes:
                log_change(change)

            time.sleep(POLL_INTERVAL)

    except KeyboardInterrupt:
        print("\nStopping CDC demo...")
        try:
            with open(CHANGE_LOG, 'r') as f:
                count = len(f.readlines())
                print(f"Final change log has {count} entries")
        except FileNotFoundError:
            print("No changes were logged.")

if __name__ == "__main__":
    main()