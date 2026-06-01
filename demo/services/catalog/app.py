import os

import uvicorn

from services.common.app_factory import create_leaf_app

app = create_leaf_app(os.getenv("SERVICE_NAME", "catalog"))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
