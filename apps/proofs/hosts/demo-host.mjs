// Tiny runner-owned host for the p0.harness-kill-demo self-proof. Serves a
// readiness probe on FIREGRID_HOST_PORT and parks until the runner kills it.
// Logs go to stderr: under ratchet targets mode the parent's stdout is
// protocol-reserved (the runner also routes child stdout to its own stderr
// as a second guard).
import { createServer } from "node:http";

const port = Number(process.env.FIREGRID_HOST_PORT ?? "0");

const server = createServer((request, response) => {
  if (request.url === "/ready") {
    response.writeHead(200, { "content-type": "text/plain" });
    response.end("ok");
    return;
  }
  response.writeHead(404);
  response.end();
});

server.listen(port, "127.0.0.1", () => {
  console.error(
    `[demo-host] ready host=${process.env.FIREGRID_HOST_ID} trial=${process.env.FIREGRID_TRIAL_ID} port=${port}`
  );
});
