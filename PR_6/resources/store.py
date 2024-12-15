from flask import Blueprint, request
from models.schemas import StoreSchema, ItemSchema
from models.store import StoreModel
from db import db

store_blueprint = Blueprint("store", __name__)
store_schema = StoreSchema()
store_list_schema = StoreSchema(many=True)


@store_blueprint.route('/', methods=['POST'])
def create_store():
    store_data = request.get_json()
    store = store_schema.load(store_data)
    new_store = StoreModel(**store)

    try:
        db.session.add(new_store)
        db.session.commit()
    except Exception as e:
        return {"message": f"Error occurred: {str(e)}"}, 500

    return store_schema.dump(new_store), 201


@store_blueprint.route('/<int:store_id>', methods=['GET'])
def get_store(store_id):
    store = StoreModel.query.get_or_404(store_id)
    return store_schema.dump(store)


@store_blueprint.route('/<int:store_id>', methods=['DELETE'])
def delete_store(store_id):
    store = StoreModel.query.get_or_404(store_id)
    db.session.delete(store)
    db.session.commit()
    return {"message": "Store deleted successfully"}


@store_blueprint.route('/', methods=['GET'])
def get_all_stores():
    stores = StoreModel.query.all()
    return store_list_schema.dump(stores)
