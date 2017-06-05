import os

import graphene

from flask import Flask, jsonify
from flask_common import Common
from flask_graphql import GraphQLView
from google.cloud import bigquery


GOOGLE_PROJECT = os.environ['GOOGLE_PROJECT']
client = bigquery.Client(project=GOOGLE_PROJECT)


def query(q, **kwargs):
    q = q.format(**kwargs)
    query_results = client.run_sync_query(q)
    query_results.use_legacy_sql = True
    query_results.run()
    rows, total_rows, page_token = query_results.fetch_data(max_results=100)

    for row in rows:
        yield row



app = Flask(__name__)
app.debug = 'DEBUG' in os.environ

common = Common(app)

class Spread(graphene.ObjectType):
    version = graphene.String()
    downloads = graphene.Int()

class Package(graphene.ObjectType):
    name = graphene.String(required=True)
    downloads = graphene.Int()
    recent_downloads = graphene.Int()
    recent_python3_adoption = graphene.Float()
    recent_python_version_spread = graphene.List(Spread)

    @graphene.resolve_only_args
    def resolve_downloads(self):
        return list(query("""
            SELECT
              COUNT(*) as total_downloads,
            FROM
              TABLE_DATE_RANGE(
                [the-psf:pypi.downloads],
                DATE_ADD(CURRENT_TIMESTAMP(), -2400, "day"),
                DATE_ADD(CURRENT_TIMESTAMP(), -1, "day")
              )
            WHERE
              file.project = '{project}'
        """, project=self.name))[0][0]

    @graphene.resolve_only_args
    def resolve_recent_downloads(self):
        return list(query("""
            SELECT
              COUNT(*) as total_downloads,
            FROM
              TABLE_DATE_RANGE(
                [the-psf:pypi.downloads],
                DATE_ADD(CURRENT_TIMESTAMP(), -31, "day"),
                DATE_ADD(CURRENT_TIMESTAMP(), -1, "day")
              )
            WHERE
              file.project = '{project}'
        """, project=self.name))[0][0]

    @graphene.resolve_only_args
    def resolve_recent_python3_adoption(self):
        return list(query("""
            SELECT
              file.project,
              ROUND(SUM(CASE WHEN REGEXP_EXTRACT(details.python, r"^([^\.]+)") = "3" THEN 1 ELSE 0 END) / COUNT(*), 3) AS percent_3,
              COUNT(*) as download_count,
            FROM
              TABLE_DATE_RANGE(
                [the-psf:pypi.downloads],
                DATE_ADD(CURRENT_TIMESTAMP(), -31, "day"),
                DATE_ADD(CURRENT_TIMESTAMP(), -1, "day")
              )
            WHERE
              file.project = '{project}'
            group by
              file.project
            ORDER BY
              download_count DESC
            LIMIT 100
        """, project=self.name))[0][1]

    @graphene.resolve_only_args
    def resolve_recent_python_version_spread(self):
        spread = list()

        for version, value in query("""
            SELECT
              REGEXP_EXTRACT(details.python, r"^([^\.]+\.[^\.]+)") as python_version,
              COUNT(*) as download_count,
            FROM
              TABLE_DATE_RANGE(
                [the-psf:pypi.downloads],
                DATE_ADD(CURRENT_TIMESTAMP(), -31, "day"),
                DATE_ADD(CURRENT_TIMESTAMP(), -1, "day")
              )
            WHERE
              file.project = '{project}'
            GROUP BY
              python_version,
            ORDER BY
              download_count DESC
            LIMIT 100
        """, project=self.name):
            s = Spread()
            s.version = version
            s.downloads = value
            spread.append(s)

        return spread

class Query(graphene.ObjectType):
    # hello = graphene.String(name=graphene.Argument(graphene.String, default_value="stranger"))
    package = graphene.Field(Package, name=graphene.String())
    # recent_top_packages = graphene.String(name=graphene.Argument(graphene.String, default_value="default"))
    recent_top_packages = graphene.List(Package)

    @graphene.resolve_only_args
    def resolve_package(self, name):
        p = Package()
        p.name = name
        return p

    @graphene.resolve_only_args
    def resolve_recent_top_packages(self):
        def gen():

            for project, downloads in query("""
                SELECT
                  file.project,
                  COUNT(*) as total_downloads,
                FROM
                  TABLE_DATE_RANGE(
                    [the-psf:pypi.downloads],
                    DATE_ADD(CURRENT_TIMESTAMP(), -31, "day"),
                    DATE_ADD(CURRENT_TIMESTAMP(), -1, "day")
                  )
                GROUP BY
                  file.project
                ORDER BY
                  total_downloads DESC
                LIMIT 250
            """):
                p = Package()
                p.name = project
                p.recent_downloads = downloads
                yield p

        return list(gen())


schema = graphene.Schema(query=Query)


app.add_url_rule('/', view_func=GraphQLView.as_view('graphql', schema=schema, graphiql=True))

# Optional, for adding batch query support (used in Apollo-Client)
app.add_url_rule('/batch', view_func=GraphQLView.as_view('graphql_batch', schema=schema, batch=True))


if __name__ == '__main__':
    common.serve()