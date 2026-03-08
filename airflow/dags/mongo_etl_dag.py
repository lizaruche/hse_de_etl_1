from __future__ import annotations

import pendulum
from pymongo import MongoClient
import psycopg2
import json

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator


def load_mongo_data():
    client = MongoClient("mongodb://de-mongo:27017/")
    db = client["app_data"]

    user_sessions = db["UserSessions"]
    event_logs = db["EventLogs"]
    support_tickets = db["SupportTickets"]
    user_recommendations = db["UserRecommendations"]
    moderation_queue = db["ModerationQueue"]

    user_sessions.delete_many({})
    event_logs.delete_many({})
    support_tickets.delete_many({})
    user_recommendations.delete_many({})
    moderation_queue.delete_many({})

    user_sessions.insert_many(
        [
            {
                "session_id": "sess_001",
                "user_id": "user_123",
                "start_time": "2024-01-10T09:00:00Z",
                "end_time": "2024-01-10T09:30:00Z",
                "pages_visited": ["/home", "/products", "/products/42", "/cart"],
                "device": {"type": "mobile"},
                "actions": ["login", "view_product", "add_to_cart", "logout"],
            },
            {
                "session_id": "sess_002",
                "user_id": "user_456",
                "start_time": "2024-01-11T10:00:00Z",
                "end_time": "2024-01-11T10:10:00Z",
                "pages_visited": ["/home", "/search?q=phone"],
                "device": {"type": "desktop"},
                "actions": ["view_product"],
            },
        ]
    )

    event_logs.insert_many(
        [
            {
                "event_id": "evt_1001",
                "timestamp": "2024-01-10T09:05:20Z",
                "event_type": "click",
                "details": {"url": "/products/42"},
            },
            {
                "event_id": "evt_1002",
                "timestamp": "2024-01-10T09:06:10Z",
                "event_type": "view",
                "details": {"url": "/cart"},
            },
        ]
    )

    support_tickets.insert_many(
        [
            {
                "ticket_id": "ticket_789",
                "user_id": "user_123",
                "status": "open",
                "issue_type": "payment",
                "messages": [
                    {
                        "sender": "user",
                        "message": "Не могу оплатить заказ.",
                        "timestamp": "2024-01-09T12:00:00Z",
                    },
                    {
                        "sender": "support",
                        "message": "Пожалуйста, уточните способ оплаты.",
                        "timestamp": "2024-01-09T13:00:00Z",
                    },
                ],
                "created_at": "2024-01-09T11:55:00Z",
                "updated_at": "2024-01-09T13:00:00Z",
            },
            {
                "ticket_id": "ticket_790",
                "user_id": "user_456",
                "status": "closed",
                "issue_type": "delivery",
                "messages": [
                    {
                        "sender": "user",
                        "message": "Где мой заказ?",
                        "timestamp": "2024-01-08T10:00:00Z",
                    },
                    {
                        "sender": "support",
                        "message": "Заказ доставлен.",
                        "timestamp": "2024-01-08T11:00:00Z",
                    },
                ],
                "created_at": "2024-01-08T09:55:00Z",
                "updated_at": "2024-01-08T11:00:00Z",
            },
        ]
    )

    user_recommendations.insert_many(
        [
            {
                "user_id": "user_123",
                "recommended_products": ["prod_101", "prod_205", "prod_333"],
                "last_updated": "2024-01-10T08:00:00Z",
            },
            {
                "user_id": "user_456",
                "recommended_products": ["prod_777", "prod_888"],
                "last_updated": "2024-01-11T07:30:00Z",
            },
        ]
    )

    moderation_queue.insert_many(
        [
            {
                "review_id": "rev_555",
                "user_id": "user_123",
                "product_id": "prod_101",
                "review_text": "Отличный товар, работает как нужно!",
                "rating": 5,
                "moderation_status": "pending",
                "flags": ["contains_images"],
                "submitted_at": "2024-01-08T10:20:00Z",
            },
            {
                "review_id": "rev_556",
                "user_id": "user_456",
                "product_id": "prod_205",
                "review_text": "Так себе качество.",
                "rating": 3,
                "moderation_status": "pending",
                "flags": [],
                "submitted_at": "2024-01-09T14:00:00Z",
            },
        ]
    )

    client.close()


def replicate_to_postgres():
    m = MongoClient("mongodb://de-mongo:27017/")
    db = m["app_data"]

    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        database="airflow",
        user="airflow",
        password="airflow",
    )

    cur = conn.cursor()

    cur.execute(
        """
        create table if not exists user_sessions (
            session_id text primary key,
            user_id text,
            start_time text,
            end_time text,
            pages_visited text[],
            device jsonb,
            actions text[]
        );
        """
    )

    cur.execute(
        """
        create table if not exists event_logs (
            event_id text primary key,
            timestamp text,
            event_type text,
            details jsonb
        );
        """
    )

    cur.execute(
        """
        create table if not exists support_tickets (
            ticket_id text primary key,
            user_id text,
            status text,
            issue_type text,
            messages jsonb,
            created_at text,
            updated_at text
        );
        """
    )

    cur.execute(
        """
        create table if not exists user_recommendations (
            user_id text primary key,
            recommended_products text[],
            last_updated text
        );
        """
    )

    cur.execute(
        """
        create table if not exists moderation_queue (
            review_id text primary key,
            user_id text,
            product_id text,
            review_text text,
            rating int,
            moderation_status text,
            flags text[],
            submitted_at text
        );
        """
    )

    cur.execute("truncate table user_sessions, event_logs, support_tickets, user_recommendations, moderation_queue;")

    for doc in db["UserSessions"].find():
        cur.execute(
            """
            insert into user_sessions (session_id, user_id, start_time, end_time, pages_visited, device, actions)
            values (%s,%s,%s,%s,%s,%s,%s)
            on conflict (session_id) do update set
                user_id = excluded.user_id,
                start_time = excluded.start_time,
                end_time = excluded.end_time,
                pages_visited = excluded.pages_visited,
                device = excluded.device,
                actions = excluded.actions;
            """,
            (
                doc.get("session_id"),
                doc.get("user_id"),
                doc.get("start_time"),
                doc.get("end_time"),
                doc.get("pages_visited") or [],
                json.dumps(doc.get("device") or {}),
                doc.get("actions") or [],
            ),
        )

    for doc in db["EventLogs"].find():
        cur.execute(
            """
            insert into event_logs (event_id, timestamp, event_type, details)
            values (%s,%s,%s,%s)
            on conflict (event_id) do update set
                timestamp = excluded.timestamp,
                event_type = excluded.event_type,
                details = excluded.details;
            """,
            (
                doc.get("event_id"),
                doc.get("timestamp"),
                doc.get("event_type"),
                json.dumps(doc.get("details") or {}),
            ),
        )

    for doc in db["SupportTickets"].find():
        cur.execute(
            """
            insert into support_tickets (ticket_id, user_id, status, issue_type, messages, created_at, updated_at)
            values (%s,%s,%s,%s,%s,%s,%s)
            on conflict (ticket_id) do update set
                user_id = excluded.user_id,
                status = excluded.status,
                issue_type = excluded.issue_type,
                messages = excluded.messages,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at;
            """,
            (
                doc.get("ticket_id"),
                doc.get("user_id"),
                doc.get("status"),
                doc.get("issue_type"),
                json.dumps(doc.get("messages") or []),
                doc.get("created_at"),
                doc.get("updated_at"),
            ),
        )

    for doc in db["UserRecommendations"].find():
        cur.execute(
            """
            insert into user_recommendations (user_id, recommended_products, last_updated)
            values (%s,%s,%s)
            on conflict (user_id) do update set
                recommended_products = excluded.recommended_products,
                last_updated = excluded.last_updated;
            """,
            (
                doc.get("user_id"),
                doc.get("recommended_products") or [],
                doc.get("last_updated"),
            ),
        )

    for doc in db["ModerationQueue"].find():
        cur.execute(
            """
            insert into moderation_queue (review_id, user_id, product_id, review_text, rating, moderation_status, flags, submitted_at)
            values (%s,%s,%s,%s,%s,%s,%s,%s)
            on conflict (review_id) do update set
                user_id = excluded.user_id,
                product_id = excluded.product_id,
                review_text = excluded.review_text,
                rating = excluded.rating,
                moderation_status = excluded.moderation_status,
                flags = excluded.flags,
                submitted_at = excluded.submitted_at;
            """,
            (
                doc.get("review_id"),
                doc.get("user_id"),
                doc.get("product_id"),
                doc.get("review_text"),
                doc.get("rating"),
                doc.get("moderation_status"),
                doc.get("flags") or [],
                doc.get("submitted_at"),
            ),
        )

    conn.commit()
    cur.close()
    conn.close()
    m.close()


def build_user_analitics():
    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        database="airflow",
        user="airflow",
        password="airflow",
    )
    cur = conn.cursor()

    cur.execute(
        """
        create table if not exists user_analitics (
            user_id text primary key,
            sessions_count int,
            total_session_seconds double precision,
            pages_visited text[],
            actions text[]
        );
        """
    )

    cur.execute("truncate table user_analitics;")

    cur.execute(
        """
        insert into user_analitics (user_id, sessions_count, total_session_seconds, pages_visited, actions)
        select
            u.user_id,
            count(*) as sessions_count,
            extract(epoch from sum((u.end_time::timestamp - u.start_time::timestamp))) as total_session_seconds,
            (
                select array_agg(distinct p)
                from user_sessions s2, unnest(s2.pages_visited) as p
                where s2.user_id = u.user_id
            ) as pages_visited,
            (
                select array_agg(distinct a)
                from user_sessions s3, unnest(s3.actions) as a
                where s3.user_id = u.user_id
            ) as actions
        from user_sessions u
        group by u.user_id;
        """
    )

    conn.commit()
    cur.close()
    conn.close()


def build_support_analitics():
    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        database="airflow",
        user="airflow",
        password="airflow",
    )
    cur = conn.cursor()

    cur.execute(
        """
        create table if not exists support_analitics (
            ticket_id text primary key,
            user_id text,
            status text,
            issue_type text,
            is_open boolean
        );
        """
    )

    cur.execute("truncate table support_analitics;")

    cur.execute(
        """
        insert into support_analitics (ticket_id, user_id, status, issue_type, is_open)
        select
            ticket_id,
            user_id,
            status,
            issue_type,
            case when lower(status) = 'closed' then false else true end as is_open
        from support_tickets;
        """
    )

    conn.commit()
    cur.close()
    conn.close()


def show_datamarts():
    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        database="airflow",
        user="airflow",
        password="airflow",
    )
    cur = conn.cursor()

    print("=== user_analitics ===")
    cur.execute("select user_id, sessions_count, total_session_seconds, pages_visited, actions from user_analitics order by user_id;")
    rows = cur.fetchall()
    for r in rows:
        print(r)

    print("=== support_analitics ===")
    cur.execute("select ticket_id, user_id, status, issue_type, is_open from support_analitics order by ticket_id;")
    rows = cur.fetchall()
    for r in rows:
        print(r)

    cur.close()
    conn.close()


with DAG(
    dag_id="mongo_etl_dag",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["mongo", "etl"],
) as dag:
    load_mongo_task = PythonOperator(
        task_id="load_mongo_data",
        python_callable=load_mongo_data,
    )

    replicate_task = PythonOperator(
        task_id="replicate_to_postgres",
        python_callable=replicate_to_postgres,
    )

    user_analitics_task = PythonOperator(
        task_id="build_user_analitics",
        python_callable=build_user_analitics,
    )

    support_analitics_task = PythonOperator(
        task_id="build_support_analitics",
        python_callable=build_support_analitics,
    )

    show_datamarts_task = PythonOperator(
        task_id="show_datamarts",
        python_callable=show_datamarts,
    )

    load_mongo_task >> replicate_task >> [user_analitics_task, support_analitics_task] >> show_datamarts_task

