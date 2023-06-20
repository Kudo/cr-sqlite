// TODO: supposedly ServiceWorkers can be killed arbitrarily?
// If that is the case we would need to make this stateless by passing all needed information
// in messages.
/*
If we passed everything..

Tab asks SW for DB.
SW would need some state for a short duration to map back who requested the DB when getting a response from who
has the DB.

*/
const mapNameToProviderPort = new Map<string, MessagePort>();

onmessage = (event) => {
  if (event.data.type === "worker_connect") {
    onWorkerConnected(event.ports[0]);
  }
};

function onWorkerConnected(workerPort: MessagePort) {
  workerPort.onmessage = async (event) => {
    if (event.ports.length) {
      // Register new port provider.
      const name = event.data;
      const providerPort = event.ports[0];
      providerPort.start();
      mapNameToProviderPort.get(name)?.close();
      mapNameToProviderPort.set(name, providerPort);

      new BroadcastChannel("SharedService").postMessage(name);
    } else {
      // Handle port provider request.
      const { name, lockId } = event.data;
      const providerPort = mapNameToProviderPort.get(name);
      if (providerPort) {
        providerPort.addEventListener(
          "message",
          (event) => {
            event.stopImmediatePropagation();
            // @ts-ignore
            workerPort.postMessage(null, event.ports);
          },
          { once: true }
        );
        providerPort.postMessage(lockId);
      }
    }
  };
}
