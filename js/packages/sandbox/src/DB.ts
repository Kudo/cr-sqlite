import serviceWorkerUrl from "./service-worker.js?url";

const registrationPromise = navigator.serviceWorker
  .register(serviceWorkerUrl, {
    scope: "/src/@vlcn.io_db-coordinator",
  })
  .then((registration) => {
    console.log(registration);
    return registration;
  });

export default class DB {
  static async open(filename: string = ":memory:"): Promise<DB> {
    const conn = new Connection(filename);
    await conn.open();
    return new DB();
  }
}

type WorkerId = string;
type CoordinateMsg =
  | {
      _tag: "db_available";
      filename: string;
      workerId: WorkerId;
    }
  | {
      _tag: "db_requested";
      filename: string;
    };

class Connection {
  #bc = new BroadcastChannel("connection-coordinate");
  #usingWorker: WorkerId | null = null;
  #messageChannel: MessageChannel | null = null;
  #swRegistration: ServiceWorkerRegistration | null = null;

  constructor(private filename: string) {
    this.#bc.onmessage = this.#broadcastReceived;
  }

  async open() {
    this.#swRegistration = await registrationPromise;
    this.#swRegistration.addEventListener;
    console.log("Installing?", this.#swRegistration.installing);
    console.log("Active?", this.#swRegistration.active);
    console.log("Waiting?", this.#swRegistration.waiting);
    // Workers will process this message,
    // try to acquire the weblock for the filename,
    // if successful (or already hold), report that it is available
    // with their worker id.
    // before reporting it is available, the worker should register a channel
    // with the service worker.
    // once we see it is available, we will create a channel to pass
    // down to the service worker who will pass it to the registered dedicated worker.
    this.#bc.postMessage({
      _tag: "db_requested",
      filename: this.filename,
    });
  }

  #broadcastReceived = (event: MessageEvent<CoordinateMsg>) => {
    switch (event.data._tag) {
      case "db_available":
        // Only care about the DB we are connecting to
        if (event.data.filename !== this.filename) {
          return;
        }
        // Only care about then event if the DB switched workers
        if (this.#usingWorker === event.data.workerId) {
          return;
        }
        this.#moveToNewWorker(event.data.workerId);
        break;
    }
  };

  // Gather pending messages.
  // re-issue them to the new worker.
  #moveToNewWorker(workerId: WorkerId) {
    this.#messageChannel = new MessageChannel();
  }
}
