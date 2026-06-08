function readPackage(pkg) {
  if (pkg.name === "@durable-streams/server" && pkg.version === "0.3.7") {
    pkg.dependencies = {
      ...pkg.dependencies,
      "@durable-streams/client": "0.2.6",
      "@durable-streams/state": "0.3.1",
    }
  }

  return pkg
}

module.exports = {
  hooks: {
    readPackage,
  },
}
