/**
 * A re-write of the DB wrapper from the lessons learned after fixing up Riffle's DB class.
 *
 * Our case, however, is more complicated given the interface to the db is async.
 * 
 * We need to take the lessons learned from improving the Riffle cache any apply them here.
We're async though so it is much more complicated.
1. Don't evict until tables used dependencies change? Maybe. We do rely on the react hook to cache this level for us.
We could move this down into the db impl itself. Or maybe let the user chose to use a cached db.
2. We can have a queue of pending queries. A write should not evict from this queue until it is time to process that write rather than evicting at write enqueue time.
3. Do this as a decorator pattern or as middleware?
4. 
 */
