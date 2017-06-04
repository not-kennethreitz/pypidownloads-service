import graphene
from flask import Flask, jsonify
from flask_common import Common



class Query(graphene.ObjectType):
    hello = graphene.String(name=graphene.Argument(graphene.String, default_value="stranger"))

    def resolve_hello(self, args, context, info):
        return 'Hello ' + args['name']

schema = graphene.Schema(query=Query)

app = Flask(__name__)
app.debug = True

common = Common(app)

@app.route('/<hello>')
@common.cache.cached(timeout=50)
def hello(hello='world'):
    result = schema.execute('{ hello }')
    return jsonify(result.data)

if __name__ == '__main__':
    common.serve()