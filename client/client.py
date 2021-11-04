import uuid

import requests

for i in range(10000):
    assets = []
    for x in range(25):
        assets.append({
            "assetId": str(uuid.uuid4()),
            "assetType": "Common Equity",
            "name": "This is a test for asset " + str(x) + " batch " + str(i),
            "entitlements": {
                "view": ["everyone", "authenticated"],
                "edit": ["admins"]
            }
        })
    resp = requests.post(
        "http://localhost:8080/api/assets",
        json={
            "assets": assets
        }
    )
    print("batch ", str(i), "time elapsed: ", str(resp.elapsed.total_seconds()))
