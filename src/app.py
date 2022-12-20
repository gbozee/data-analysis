from collections import Counter
from src.settings import database


async def db_query(sql, values):
    result = []
    async with database:
        result = await database.fetch_all(query=sql, values=values)
    return result


async def booked_customers(min_no_of_bookings=0):
    sql = """
    select email, count("order") as booking_count,"id"
	from auth_user A 
    	left join bookings_booking B on B.user_id = A.id
    group by email, id
    having count("order") > :min_count;
    """
    result = await db_query(sql, {"min_count": min_no_of_bookings})
    return [
        {"email": x["email"], "booking_count": x["booking_count"], "id": x["id"]}
        for x in result
    ]


async def tutors_from_bookings_by_customers(min_no=0):
    users = await booked_customers(min_no)
    user_ids = [x["id"] for x in users]
    payload = ",".join([str(x) for x in user_ids])
    sql = """
    select 
        "order", 
        user_id, 
        ts_id,  
        tutor_id, 
        created
    from "bookings_booking" B
    where user_id in ({})
    """.format(
        payload
    )
    bookings = await db_query(sql, {})
    return [
        {"order": x["order"], "user_id": x["user_id"], "tutor_id": x["tutor_id"]}
        for x in bookings
        if x["tutor_id"]
    ]


async def group_users_with_orders(min_no=0, group=False):
    users = {}
    bookings = await tutors_from_bookings_by_customers(min_no)
    for record in bookings:
        existing = users.get(record["user_id"]) or []
        existing.append(record)
        users[record["user_id"]] = existing

    results = [
        {
            "user_id": key,
            "tutor_ids": [x["tutor_id"] for x in values],
            "orders_with_tutor": [(x["tutor_id"], x["order"]) for x in values],
        }
        for key, values in users.items()
    ]
    if group:
        return [
            {
                "user_id": key,
                "tutor_ids": dict(Counter([x["tutor_id"] for x in values])),
            }
            for key, values in users.items()
        ]
    return results
