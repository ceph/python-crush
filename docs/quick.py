import json
from crush import Crush

crushmap = """
{
  "trees": {
    "dc1": {
      "~type~": "root", "~id~": -1,
      "host0": {
        "~type~": "host", "~id~": -2,
        "device0": { "~id~": 0, "~weight~": 1.0 },
        "device1": { "~id~": 1, "~weight~": 2.0 }
      },
      "host1": {
        "~type~": "host", "~id~": -3,
        "device2": { "~id~": 2, "~weight~": 1.0 },
        "device3": { "~id~": 3, "~weight~": 2.0 }
      },
      "host2": {
        "~type~": "host", "~id~": -4,
        "device4": { "~id~": 4, "~weight~": 1.0 },
        "device5": { "~id~": 5, "~weight~": 2.0 }
      }
    }
  },
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
