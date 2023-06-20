import DB from "./DB";
import sharedWorkerUrl from "./shared-worker.js?url";

const db = DB.open("a-file");

console.log("start sared");
const sharedWorker = new SharedWorker(sharedWorkerUrl, { type: "module" });
/**
 * Algorithm:
 * 1. Each tab spawns a dedicated worker
 * 2. Each dedicated worker waits for db open events
 * 3. On open event, try to acquire a weblock for the given db name.
 *
 * We need some sort of central "opener" to coordinate message port passing.
 * The DB could already be open in a different tab is the issue and we need to find it if so.
 *
 * We attempt to open the DB by:
 * 1. Acquiring the weblock for it
 * 2. Setting up a migration listener
 * 3. Broadcasting a request for msg ports
 *
 * If we get the lock we notify ourselves thru the same listener.
 *
 * Main tab:
 * 1. Create message channel
 * 2. Start worker
 * 3. Send worker other end of message channel
 * 4. Worker:
 *    1. Registers for DB Open event message against a broadcast channel
 *    2. On receipt, tries to claim a weblock for that db name.
 * 2. On DB Open:
 *    1. Register a listener for DB open event with our worker
 *    2. Broadcast event
 *
 * Workers all need to ping the service worker. Workers would pass around msg ports depending on lock acquisition.
 * Well if worker dies msg ports die with it.
 *
 * ServiceWorker is the pub-sub channel.
 *
 * Tab tells ServiceWorker, SW tells workers when a new DB is requested. Worker that "gets that db" or "has that db" responds to SW.
 * This response to SW includes a message port for each interested tab.
 * SW sends these ports back out to all the registered tabs...
 *
 * Maybe need a bit more.
 *
 * Tab -ask for db-> SW -ask for db-> Workers
 *
 * Worker -has db-> SW -db avail, worker num-> Tabs
 *
 * Tab -msg port for db, worker num-> SW -msg port-> Worker
 *
 * SW isn't intended to be stateful.. but if it has message channels set up, does it ever die?
 * It would seem it should not.
 *
 * If not, we can do something like: https://github.com/rhashimoto/wa-sqlite/blob/master/demo/SharedService/SharedService_SharedWorker.js
 */
