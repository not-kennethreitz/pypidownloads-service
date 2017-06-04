import graphene

from flask import Flask, jsonify
from flask_common import Common
from flask_graphql import GraphQLView


app = Flask(__name__)
app.debug = True

common = Common(app)


class Query(graphene.ObjectType):
    hello = graphene.String(name=graphene.Argument(graphene.String, default_value="stranger"))

    def resolve_hello(self, args, context, info):
        return 'Hello ' + args['name']


schema = graphene.Schema(query=Query)

app.add_url_rule('/', view_func=GraphQLView.as_view('graphql', schema=schema, graphiql=True))

# Optional, for adding batch query support (used in Apollo-Client)
app.add_url_rule('/batch', view_func=GraphQLView.as_view('graphql_batch', schema=schema, batch=True))


if __name__ == '__main__':
    common.serve()