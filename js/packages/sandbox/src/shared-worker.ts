console.log("in shared...");
console.log("do we have opfs access?!");
// Test that we can actually pass a message port from tab to service worker to worker first.
navigator.storage.getDirectory().then((opfsRoot) => {
  console.log(opfsRoot);
});
// A FileSystemDirectoryHandle whose type is "directory"
// and whose name is "".
