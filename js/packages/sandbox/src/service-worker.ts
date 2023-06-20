self.addEventListener("install", function (event) {
  (event as any).waitUntil(Promise.resolve());
});

self.addEventListener("activate", (event) => {
  // @ts-ignore
  event.waitUntil(clients.claim());
});

const mapNameToProviderPort = new Map<string, MessagePort>();

onmessage = (event) => {
  console.log("sw got a msg", event);
  // we will want to `event.awaitUntil` as we handle port passing.
};

// function onWorkerConnected(workerPort: MessagePort) {
//   workerPort.onmessage = async (event) => {
//     if (event.ports.length) {
//       // Register new port provider.
//       const name = event.data;
//       const providerPort = event.ports[0];
//       providerPort.start();
//       mapNameToProviderPort.get(name)?.close();
//       mapNameToProviderPort.set(name, providerPort);

//       new BroadcastChannel("SharedService").postMessage(name);
//     } else {
//       // Handle port provider request.
//       const { name, lockId } = event.data;
//       const providerPort = mapNameToProviderPort.get(name);
//       if (providerPort) {
//         providerPort.addEventListener(
//           "message",
//           (event) => {
//             event.stopImmediatePropagation();
//             // @ts-ignore
//             workerPort.postMessage(null, event.ports);
//           },
//           { once: true }
//         );
//         providerPort.postMessage(lockId);
//       }
//     }
//   };
// }

// console.log("do we have opfs access?!");
// // Test that we can actually pass a message port from tab to service worker to worker first.
// navigator.storage.getDirectory().then((opfsRoot) => {
//   console.log(opfsRoot);
// });
// // A FileSystemDirectoryHandle whose type is "directory"
// // and whose name is "".
