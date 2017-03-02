import json
from crush import Crush

crushmap = """
{
  "trees": [
    {
      "type": "root", "name": "dc1", "id": -1,
      "children": [
        {
         "type": "host", "name": "host0", "id": -2,
         "children": [
          { "id": 0, "name": "device0", "weight": 1.0 },
          { "id": 1, "name": "device1", "weight": 2.0 }
         ]
        },
        {
         "type": "host", "name": "host1", "id": -3,
         "children": [
          { "id": 2, "name": "device2", "weight": 1.0 },
          { "id": 3, "name": "device3", "weight": 2.0 }
         ]
        },
        {
         "type": "host", "name": "host2", "id": -4,
         "children": [
          { "id": 4, "name": "device4", "weight": 1.0 },
          { "id": 5, "name": "device5", "weight": 2.0 }
         ]
        }
      ]
    }
  ],
  "rules": {
    "data": [
      [ "take", "dc1" ],
      [ "chooseleaf", "firstn", 0, "type", "host" ],
      [ "emit" ]
    ]
  }
}

"""

c = Crush()
c.parse(json.loads(crushmap))
print c.map(rule="data", value=1234, replication_count=1)
print c.map(rule="data", value=1234, replication_count=2)
