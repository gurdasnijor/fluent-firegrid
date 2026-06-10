/* global console, process */
import { createServer } from "node:http"
import { readFile } from "node:fs/promises"
import { extname, join, normalize } from "node:path"
import { fileURLToPath, URL } from "node:url"

const root = fileURLToPath(new URL("..", import.meta.url))
const host = process.env.OPENAPI_VIEW_HOST ?? "127.0.0.1"
const port = Number(process.env.OPENAPI_VIEW_PORT ?? 4011)

const contentTypes = new Map([
  [".yaml", "application/yaml; charset=utf-8"],
  [".yml", "application/yaml; charset=utf-8"],
  [".json", "application/json; charset=utf-8"],
])

const html = String.raw`<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Durable Streams OpenAPI</title>
    <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css" />
    <style>
      body { margin: 0; background: #fff; }
      .swagger-ui .topbar { display: none; }
    </style>
  </head>
  <body>
    <div id="swagger-ui"></div>
    <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
    <script>
      SwaggerUIBundle({
        dom_id: "#swagger-ui",
        url: "/openapi/durable-streams.yaml",
        deepLinking: true,
        layout: "BaseLayout"
      });
    </script>
  </body>
</html>`

const server = createServer(async (request, response) => {
  try {
    const url = new URL(request.url ?? "/", `http://${request.headers.host ?? `${host}:${port}`}`)

    if (url.pathname === "/" || url.pathname === "/index.html") {
      response.writeHead(200, { "content-type": "text/html; charset=utf-8" })
      response.end(html)
      return
    }

    if (url.pathname.startsWith("/openapi/")) {
      const relative = normalize(url.pathname.slice(1))
      if (!relative.startsWith("openapi/")) {
        response.writeHead(400)
        response.end("Bad request")
        return
      }

      const file = join(root, relative)
      const body = await readFile(file)
      response.writeHead(200, {
        "content-type": contentTypes.get(extname(file)) ?? "application/octet-stream",
      })
      response.end(body)
      return
    }

    response.writeHead(404)
    response.end("Not found")
  } catch (error) {
    response.writeHead(500, { "content-type": "text/plain; charset=utf-8" })
    response.end(error instanceof Error ? error.message : String(error))
  }
})

server.listen(port, host, () => {
  console.log(`OpenAPI viewer listening on http://${host}:${port}`)
  console.log(`Spec: http://${host}:${port}/openapi/durable-streams.yaml`)
})
