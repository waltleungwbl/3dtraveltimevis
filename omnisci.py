from pymapd import connect
import csv
import json

user_str = 'Y87EC84159104494BA58'
password_str = '5aQmKfgj9p9Qp8ZRgGvjwZR7KWK8GXSr0GTer08I'
host_str = 'use2-api.mapd.cloud'
dbname_str = 'mapd'
connection = connect(user=user_str, password=password_str, host=host_str, dbname=dbname_str, port=443, protocol='https')
c = connection.cursor()

size = 10000
location = input('')
query = f'SELECT * FROM uber_movement_data where uber_movement_data.sourceid = {location}'
df = connection.execute(query)
query = f'CREATE VIEW IF NOT EXISTS u{location} AS SELECT * FROM uber_movement_data where uber_movement_data.sourceid = {location}'
df = connection.execute(query)

query = f'CREATE VIEW IF NOT EXISTS combinedDestination{location} AS SELECT * FROM u{location} JOIN bay_area_taz on u{location}.dstid=bay_area_taz.uber_id'
df = connection.execute(query)
query = f'SELECT * from combinedDestination{location}'
df = connection.execute(query)
headers = [item.name for item in df.description]
result = df.fetchmany(size=size)

with open(f'{location}.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
    writer.writerow(headers)
    for row in result:
        writer.writerow(row)

with open(f'./{location}.csv', 'r') as f:
    reader = csv.DictReader(f, delimiter=',', quotechar="|")
    features = []
    for line in reader:
        line = next(reader)
        omnisci = line.pop("omnisci_geo", None)
        coord_pairs_str = eval(omnisci[14:-1].replace(" ", ",").replace("(", "[").replace(")", "]"))
        output = []
        for l in coord_pairs_str:
            output_l = []
            lst = iter(l)
            for x in lst:
                y = next(lst)
                output.append([x, y])
            if output_l:
                output.append(output_l)
        complete = {"type":"Feature","geometry":{"type":"Polygon","coordinates":[output]}, "properties": line}
        features.append(complete)
    final_output = {"type": "FeatureCollection", "features": features}
    print(json.dumps(final_output))
