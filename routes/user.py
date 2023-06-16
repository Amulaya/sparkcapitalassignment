from schemas.user import User
from fastapi import APIRouter
from models.user import users
from config.database import conn


user = APIRouter()


@user.get('/')
def fetch_user():
    query = conn.execute(users.select()).fetchall()
    return query


@user.get('/{id}')
def fetch_single_user(id: int):
    query = conn.execute(users.select().where(users.c.id == id)).first()
    return query


@user.post('/')
def create_user(usr: User):
    conn.execute(users.insert().values(
        name=usr.name,
        email=usr.email,
        password=usr.password
    ))
    return conn.execute(users.select()).fetchall()


@user.put('/{id}')
def update_user(id: int, user: User):
    conn.execute(users.update().values(
        name=user.name,
        email=user.email,
        password=user.password
    ).where(users.c.id == id))
    return conn.execute(users.select()).fetchall()


@user.delete('/{id}')
def delete_user(id: int):
    conn.execute(users.delete().where(users.c.id == id))
    return conn.execute(users.select()).fetchall()

