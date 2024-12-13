from flask.views import MethodView
from flask_smorest import Blueprint
from flask_jwt_extended import create_access_token
from passlib.hash import pbkdf2_sha256
from db import db
from models.user import UserModel
from sqlalchemy.exc import IntegrityError
from flask import request

user_blueprint = Blueprint("Users", __name__, description="Operations on users")

@user_blueprint.route("/register")
class UserRegister(MethodView):
    def post(self):
        user_data = request.get_json()
        user = UserModel(
            username=user_data["username"],
            password=pbkdf2_sha256.hash(user_data["password"]),
        )
        try:
            db.session.add(user)
            db.session.commit()
        except IntegrityError:
            return {"message": "A user with that username already exists."}, 400
        return {"message": "User created successfully."}, 201

@user_blueprint.route("/login")
class UserLogin(MethodView):
    def post(self):
        user_data = request.get_json()
        user = UserModel.query.filter_by(username=user_data["username"]).first()

        if user and pbkdf2_sha256.verify(user_data["password"], user.password):
            access_token = create_access_token(identity=user.id)
            return {"access_token": access_token}, 200

        return {"message": "Invalid credentials"}, 401
