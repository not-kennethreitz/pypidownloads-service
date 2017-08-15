import os

import graphene

from flask import Flask
from flask_graphql import GraphQLView
from flask_basicauth import BasicAuth
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

app.config['BASIC_AUTH_USERNAME'] = 'kennethreitz'
app.config['BASIC_AUTH_PASSWORD'] = 'kennethreitz'
app.config['BASIC_AUTH_FORCE'] = True

basic_auth = BasicAuth(app)
# common = Common(app)


class VersionSpread(graphene.ObjectType):
    version = graphene.String()
    downloads = graphene.Int()
    percent = graphene.Float()

    @graphene.resolve_only_args
    def resolve_percent(self):
        return self.downloads / self.total

class RegionSpread(graphene.ObjectType):
    region = graphene.String()
    downloads = graphene.Int()
    percent = graphene.Float()

    @graphene.resolve_only_args
    def resolve_percent(self):
        return self.downloads / self.total

class Package(graphene.ObjectType):
    name = graphene.String(required=True)
    recent_downloads = graphene.Int()
    recent_python3_adoption = graphene.Float()
    recent_python_version_spread = graphene.List(VersionSpread)
    recent_region_spread = graphene.List(RegionSpread)

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
              ROUND(SUM(CASE WHEN REGEXP_EXTRACT(details.python, r"^([^\.]+)") = "3" THEN 1 ELSE 0 END) / COUNT(*), 16) AS percent_3,
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
            s = VersionSpread()
            s.version = version
            s.downloads = value
            spread.append(s)


        # Add .total to every RegionSpread.
        total = sum([s.downloads for s in spread])

        for i, s in enumerate(list(spread)):
            s.total = total
            spread[i] = s

        return spread

        return spread

    @graphene.resolve_only_args
    def resolve_recent_region_spread(self):
        spread = list()

        for region, value in query("""
            SELECT
              country_code,
              COUNT(*) as downloads,
            FROM
              TABLE_DATE_RANGE(
                [the-psf:pypi.downloads],
                DATE_ADD(CURRENT_TIMESTAMP(), -31, "day"),
                DATE_ADD(CURRENT_TIMESTAMP(), -1, "day")
              )
            WHERE
              file.project = '{project}'
            GROUP BY
              country_code
            ORDER BY
              downloads DESC
            LIMIT 100
        """, project=self.name):
            s = VersionSpread()
            s.region = region
            s.downloads = value
            s.total = None
            spread.append(s)

        # Add .total to every RegionSpread.
        total = sum([s.downloads for s in spread])

        for i, s in enumerate(list(spread)):
            s.total = total
            spread[i] = s

        return spread

class Query(graphene.ObjectType):
    package = graphene.Field(Package, name=graphene.String())
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
    # common.serve()
    app.run()