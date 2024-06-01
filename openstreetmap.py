import neo4j
import osmnx as ox


AURA_CONNECTION_URI = "neo4j+s://798f1c57.databases.neo4j.io:7687"
AURA_USERNAME = "neo4j"
AURA_PASSWORD = "SVx6ZBk6WLweZZ6U-cdchwxa0xMJSOXyhFpYIpLRp5Y"

#get coordinates and data from one city
ADDRESS = "Wuppertal, NRW, Germany"
G = ox.graph_from_place(ADDRESS, network_type="drive")

gdf_nodes, gdf_relationships = ox.graph_to_gdfs(G)
gdf_nodes.reset_index(inplace=True)
gdf_relationships.reset_index(inplace=True)



driver = neo4j.GraphDatabase.driver(AURA_CONNECTION_URI, auth=(AURA_USERNAME, AURA_PASSWORD))

#fig, ax = ox.plot_graph(G)

constraint_query = "CREATE CONSTRAINT IF NOT EXISTS FOR (i:Intersection) REQUIRE i.osmid IS UNIQUE"

rel_index_query = "CREATE INDEX IF NOT EXISTS FOR ()-[r:ROAD_SEGMENT]-() ON r.osmids"

address_constraint_query = "CREATE CONSTRAINT IF NOT EXISTS FOR (a:Address) REQUIRE a.id IS UNIQUE"

point_index_query = "CREATE POINT INDEX IF NOT EXISTS FOR (i:Intersection) ON i.location"

def create_indices(tx):
    results = tx.run(constraint_query)
    results = tx.run(rel_index_query)
    results = tx.run(address_constraint_query)
    results = tx.run(point_index_query)

#insert data
#retrieve data from node and alter the attributes
#row is a geodataframe containing the columns:
#osmid          y           x   ref            highway  street_count                     geometry
#and then parse it into convertable data
INSERT_NODES = '''
    UNWIND $rows AS row
    WITH row WHERE row.osmid IS NOT NULL
    MERGE (i:Intersection {osmid: row.osmid})
        SET i.location = 
         point({latitude: row.y, longitude: row.x }),
            i.ref = row.ref,
            i.highway = row.highway,
            i.street_count = toInteger(row.street_count)
    RETURN COUNT(*) as total
    '''

INSERT_RELS = '''
    UNWIND $rows AS road
    MATCH (u:Intersection {osmid: road.u})
    MATCH (v:Intersection {osmid: road.v})
    MERGE (u)-[r:ROAD_SEGMENT {osmid: road.osmid}]->(v)
        SET r.oneway = road.oneway,
            r.lanes = road.lanes,
            r.ref = road.ref,
            r.name = road.name,
            r.highway = road.highway,
            r.max_speed = road.maxspeed,
            r.length = toFloat(road.length)
    RETURN COUNT(*) AS total
    '''


def insert_data(tx, query, rows, batch_size=10000):
    total = 0
    batch = 0
    while batch * batch_size < len(rows):
        print(type(rows))
        print(type(batch_size))
        #give amount of rows as a parameter to query
        results = tx.run(query, parameters = {'rows': rows[batch*batch_size:(batch+1)*batch_size].to_dict('records')}).data()
        print(results)
        total += results[0]['total']
        batch += 1

with driver.session() as session:
    driver.verify_connectivity()
    #expects function which executes queries inside of a transaction
    session.execute_write(create_indices)
    print("inserting nodes")
    session.execute_write(insert_data, INSERT_NODES, gdf_nodes.drop(columns=['geometry']))
    #print(inserting relationships)
    session.execute_write(insert_data, INSERT_RELS, gdf_relationships.drop(columns=['geometry'])) 

#intersections are stored, but not the actual addresses and their properties (streetname)
#meaning: names are NaN right now
#source.geojson was downloaded from openaddresses 
#you might need to turn the option to load from files on in apoc.conf
def retrieve_addresses(tx): 
    call = '''
        :auto
        CALL apoc.load.json("https://raw.githubusercontent.com/Botchkarev/nosql_db/master/source.geojson(2)") YIELD value
        CALL {
        WITH value
        CREATE (a:Address)
        SET a.location = 
            point(
            {
                latitude: value.geometry.coordinates[1], 
                longitude: value.geometry.coordinates[0]
            }),
            a.full_address = value.properties.number + " " + 
                            value.properties.street + " " + 
                            value.properties.city + ", CA " + 
                            value.properties.postcode
        SET a += value.properties
        } IN TRANSACTIONS OF 10000 ROWS'''
    result = tx.run(call)

def connect_addresses(tx):
    result = tx.run('''MATCH (a:Address) WITH a LIMIT 1
    CALL {
    WITH a
    MATCH (i:Intersection)
    USING INDEX i:Intersection(location)
    WHERE point.distance(i.location, a.location) < 200

    WITH i
    ORDER BY point.distance(p.location, a.location) ASC 
    LIMIT 1
    RETURN i
    } WITH a, i
    MERGE(a)-[r:NEAREST_INTERSECTION]->(i)
    SET r.length = point.distance(a.location, i.location)''')

with driver.session() as session:
    session.execute_write(retrieve_addresses)

#finally, get the shortest route between two points of interest
#SEARCH_QUERY = '''MATCH (a:Address)-[:NEAREST_INTERSECTION]->(source:Intersection)
#WHERE a.full_address CONTAINS " "''' + POINT_A + ADDRESS + '''"
#MATCH (poi:Address)-[:NEAREST_INTERSECTION]->(dest:Intersection) 
#WHERE poi.full_address CONTAINS "39 GRAND BLVD ''' + POINT_B + ADDRESS + '''"
#CALL apoc.algo.dijkstra(source, dest, "ROAD_SEGMENT", "length") 
#YIELD weight, path
#RETURN *'''

#driver = neo4j.GraphDatabase.driver(AURA_CONNECTION_URI, auth=(AURA_USERNAME, AURA_PASSWORD))
#simple read query, no need for transaction
#with driver.session() as session:
#    result = session.execute_read(SEARCH_QUERY)
#    print(result)
#    driver.close()