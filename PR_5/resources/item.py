from flask.views import MethodView
from flask_smorest import Blueprint
from marshmallow import Schema, fields
from flask_jwt_extended import jwt_required
from db import db
from models import ItemModel


class ItemSchema(Schema):
    id = fields.Int(dump_only=True)
    name = fields.Str(required=True)
    price = fields.Float(required=True)
    store_id = fields.Int(required=True)

class ItemUpdateSchema(Schema):
    name = fields.Str()
    price = fields.Float()
    store_id = fields.Int()


item_blueprint = Blueprint("Items", __name__, description="Operations on items")

@item_blueprint.route("/item/<int:item_id>")
class Item(MethodView):
    @jwt_required()
    @item_blueprint.response(200, ItemSchema)
    def get(self, item_id):
        item = ItemModel.query.get_or_404(item_id)
        return item

    @jwt_required()
    def delete(self, item_id):
        item = ItemModel.query.get_or_404(item_id)
        db.session.delete(item)
        db.session.commit()
        return {"message": "Item deleted"}, 200

    @jwt_required()
    @item_blueprint.arguments(ItemUpdateSchema)
    @item_blueprint.response(200, ItemSchema)
    def put(self, item_data, item_id):
        item = ItemModel.query.get(item_id)
        if item:
            item.name = item_data["name"]
            item.price = item_data["price"]
            item.store_id = item_data["store_id"]
        else:
            item = ItemModel(id=item_id, **item_data)
        db.session.add(item)
        db.session.commit()
        return item

@item_blueprint.route("/item")
class ItemList(MethodView):
    @jwt_required()
    @item_blueprint.response(200, ItemSchema(many=True))
    def get(self):
        return ItemModel.query.all()

    @jwt_required()
    @item_blueprint.arguments(ItemSchema)
    @item_blueprint.response(201, ItemSchema)
    def post(self, item_data):
        item = ItemModel(**item_data)
        db.session.add(item)
        db.session.commit()
        return item
