/* Web Worker: derive ONE default address from a base seed + index + keysPerLevel (64 classic,
   128 block-as-keyuses). Each key is independent (~0.5–1M SHA3 hashes), so a small worker pool
   derives the wallet in parallel instead of blocking the UI for minutes. Falls back to inline
   derivation if workers are blocked. */
importScripts("sha3.js", "derive.js");

self.onmessage = function (e) {
  var d = e.data;                       // { baseSeedHex, index, keysPerLevel }
  try {
    var seed = self.KeyUsesDerive._fromHex(d.baseSeedHex);
    var a = self.KeyUsesDerive.deriveAddress(seed, d.index, null, d.keysPerLevel);
    self.postMessage({ ok: true, index: d.index, keysPerLevel: a.keysPerLevel, address: a.hex, miniaddress: a.mx, publickey: a.publickey });
  } catch (err) {
    self.postMessage({ ok: false, index: d.index, keysPerLevel: d.keysPerLevel, error: String(err && err.message || err) });
  }
};
