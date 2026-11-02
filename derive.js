/* KeyUses Phrase — byte-exact JS port of Minima's phrase → seed → default addresses,
   in both tree shapes: classic (64 WOTS keys/level) and block-as-keyuses (128 keys/level,
   minimaCore -blockaskeyuses). Same seed, different addresses per shape.
   Ground truth: minima-core (BIP39, Wallet, TreeKey/TreeKeyNode, Winternitz, Address, MMR,
   MiniData/MiniNumber serialization) and the freezepeach-ios Swift reimplementation, both
   validated against the Java engine's goldens. Reference command: `vault action:testphrase`.

   FUND-ADJACENT: a one-bit deviation gives a wrong address. Do not "optimise" the hashing or
   serialization without re-checking every golden in derive.test.js.

   Depends on a global `sha3_256` (js-sha3) exposing `.array(bytes) -> number[32]`.
   Works in the browser, in a Web Worker (importScripts), and in Node (require).           */
(function (root) {
  "use strict";

  // SHA3-256 (FIPS-202, NOT keccak). js-sha3's sha3_256.array returns a plain number[] of bytes.
  var _sha3 = (typeof sha3_256 !== "undefined") ? sha3_256
            : (typeof require !== "undefined") ? require("js-sha3").sha3_256
            : null;
  function setSha3(fn) { _sha3 = fn; }
  function SHA3(bytes) { return _sha3.array(bytes); }   // number[] in, number[32] out

  /* ---------- Minima Streamable serialization (java.io.DataOutputStream semantics) ---------- */
  // MiniData: writeInt(len) [4-byte big-endian] then the raw bytes.
  function serMiniData(bytes) {
    var n = bytes.length;
    return [(n >>> 24) & 0xFF, (n >>> 16) & 0xFF, (n >>> 8) & 0xFF, n & 0xFF].concat(bytes);
  }
  // Java BigInteger(n).toByteArray(): minimal big-endian two's-complement, >=1 byte, sign/pad byte
  // when the magnitude's top bit would flip the sign. Non-negative n only (key indices).
  function bigIntegerToByteArray(n) {
    if (n === 0) return [0];
    var out = [];
    while (n > 0) { out.unshift(n & 0xFF); n = Math.floor(n / 256); }
    if (out[0] & 0x80) out.unshift(0);   // positive number whose top bit is set -> prepend 0x00
    return out;
  }
  // MiniNumber (integer): writeByte(scale=0), writeByte(unscaledLen), unscaled bytes.
  function serMiniNumber(n) {
    var u = bigIntegerToByteArray(n);
    return [0, u.length & 0xFF].concat(u);
  }
  // `new MiniData(new BigInteger(Integer.toString(n)))` — minimal UNSIGNED big-endian bytes; 0 -> [0].
  // (BigInteger.toByteArray() then strip a single leading 0x00 when len>1; equals minimal unsigned BE.)
  function modifierBytes(n) {
    if (n === 0) return [0];
    var out = [];
    while (n > 0) { out.unshift(n & 0xFF); n = Math.floor(n / 256); }
    return out;
  }

  // hashObjects(...serializedParts): SHA3-256 over the concatenated serializations.
  function hashConcat(parts) {
    var buf = [];
    for (var i = 0; i < parts.length; i++) buf = buf.concat(parts[i]);
    return SHA3(buf);
  }

  /* ---------- hex ---------- */
  var HEX = "0123456789ABCDEF";
  function toHexUpper(bytes) {
    var s = "";
    for (var i = 0; i < bytes.length; i++) s += HEX[(bytes[i] >> 4) & 0xF] + HEX[bytes[i] & 0xF];
    return s;
  }
  function fromHex(hex) {
    var s = hex.replace(/^0x/i, ""), out = [];
    for (var i = 0; i < s.length; i += 2) out.push(parseInt(s.substr(i, 2), 16));
    return out;
  }
  function utf8Bytes(str) {
    // MiniString serializes UTF-8 bytes; the script is ASCII so this is exact.
    if (typeof TextEncoder !== "undefined") return Array.prototype.slice.call(new TextEncoder().encode(str));
    var out = [];
    for (var i = 0; i < str.length; i++) {
      var c = str.charCodeAt(i);
      if (c < 0x80) out.push(c);
      else if (c < 0x800) { out.push(0xC0 | (c >> 6), 0x80 | (c & 0x3F)); }
      else { out.push(0xE0 | (c >> 12), 0x80 | ((c >> 6) & 0x3F), 0x80 | (c & 0x3F)); }
    }
    return out;
  }

  /* ---------- BIP39 phrase -> 32-byte base seed ---------- */
  // seed = SHA3-256( utf8(phrase-string) ). NO PBKDF2, salt or checksum.
  //  - BIP39 mode: cleanSeedPhrase first (words prefix-expanded, UPPERCASE, single-spaced).
  //  - anyphrase mode: hash the phrase verbatim (mirrors `restore ... anyphrase:true`).
  function seedFromPhrase(phrase, anyphrase) {
    var toHash = anyphrase ? phrase : cleanSeedPhrase(phrase);
    return SHA3(utf8Bytes(toHash));
  }

  // Port of BIP39.cleanSeedPhrase: split on whitespace, lowercase each token, <4 chars => exact
  // word match, >=4 chars => first word that startsWith(token); join single-spaced then UPPERCASE.
  // Throws on unknown/too-short tokens, exactly like the node.
  function cleanSeedPhrase(phrase) {
    var toks = String(phrase).trim().split(/\s+/).filter(function (t) { return t.length; });
    if (!toks.length) throw new Error("Empty seed phrase.");
    var list = root.KeyUsesWordList;
    if (!list) throw new Error("BIP39 word list not loaded.");
    var out = [];
    for (var i = 0; i < toks.length; i++) {
      var tok = toks[i].toLowerCase();
      if (tok.length < 3) throw new Error("Word too short: " + tok);
      var found = null, j;
      if (tok.length < 4) {
        for (j = 0; j < list.length; j++) if (tok === list[j]) { found = list[j]; break; }
      } else {
        for (j = 0; j < list.length; j++) if (list[j].indexOf(tok) === 0) { found = list[j]; break; }
      }
      if (found === null) throw new Error("Unknown BIP39 word: " + tok);
      out.push(found);
    }
    return out.join(" ").toUpperCase();
  }

  /* ---------- Winternitz OTS (BouncyCastle GMSS, w=8, SHA3-256, mdsize=32) ---------- */
  var WOTS_KEYSIZE = 34;   // messagesize 32 + ceil(checksumsize 14 / 8)
  var WOTS_CHAIN = 255;    // (1<<w) - 1

  // GMSSRandom.nextSeed: rand = SHA3(seed); seed = seed + rand + 1 (mod 2^256, little-endian); return rand.
  function gmssNextSeed(seed) {
    var rand = SHA3(seed);
    var carry = 0, i;
    for (i = 0; i < seed.length; i++) {           // seed += rand  (LE, index 0 = least significant)
      var t = seed[i] + rand[i] + carry; seed[i] = t & 0xFF; carry = t >> 8;
    }
    carry = 1;                                     // seed += 1  (LE)
    for (i = 0; i < seed.length && carry; i++) {
      var u = seed[i] + carry; seed[i] = u & 0xFF; carry = u >> 8;
    }
    return rand;
  }
  // 32-byte WOTS public key from a >=32-byte seed.
  function wotsPublicKey(seed) {
    var dummy = seed.slice(0, 32);
    var pub = [];
    for (var b = 0; b < WOTS_KEYSIZE; b++) {
      var block = gmssNextSeed(dummy);             // 34 private blocks
      var help = SHA3(block);                      // chain to 255 applications
      for (var c = 0; c < WOTS_CHAIN - 1; c++) help = SHA3(help);
      pub = pub.concat(help);
    }
    return SHA3(pub);                              // SHA3 over the 1088-byte concat
  }

  /* ---------- MMR (hash-sum tree, all sums 0 for keys/addresses) ---------- */
  function mmrLeaf(data) {                          // CreateMMRDataLeafNode(data, ZERO), prefix 0
    return hashConcat([serMiniNumber(0), serMiniData(data), serMiniNumber(0)]);
  }
  function mmrParent(left, right) {                 // CreateMMRDataParentNode(left, right), prefix 1
    return hashConcat([serMiniNumber(1), serMiniData(left), serMiniData(right), serMiniNumber(0)]);
  }
  function mmrPerfectRoot(leaves) {                 // power-of-two leaf count -> single peak
    var level = leaves;
    while (level.length > 1) {
      var next = [];
      for (var i = 0; i < level.length; i += 2) next.push(mmrParent(level[i], level[i + 1]));
      level = next;
    }
    return level[0];
  }

  /* ---------- TreeKey: root pubkey = MMR over keysPerLevel WOTS pubkeys ----------
     classic nodes:          64 keys/level, 3 levels
     -blockaskeyuses nodes: 128 keys/level, 4 levels  (same seed -> DIFFERENT pubkey/address)
     The level count never enters the pubkey — only the TOP node's keysPerLevel leaves do. */
  // privateSeed for key `index`: SHA3( ser(MiniData(baseSeed)) || ser(MiniData(modifier)) ).
  function privateSeed(baseSeed, index) {
    return hashConcat([serMiniData(baseSeed), serMiniData(modifierBytes(index))]);
  }
  // Returns the 32-byte tree public key. onLeaf(j) is an optional progress callback.
  function treeKeyPublicKey(privseed, onLeaf, keysPerLevel) {
    var kpl = keysPerLevel || 64;
    var leaves = [];
    for (var j = 0; j < kpl; j++) {
      var seedJ = hashConcat([serMiniNumber(j), serMiniData(privseed)]);  // seed_j
      leaves.push(mmrLeaf(wotsPublicKey(seedJ)));
      if (onLeaf) onLeaf(j);
    }
    return mmrPerfectRoot(leaves);
  }

  /* ---------- Address: pubkey -> RETURN SIGNEDBY(...) -> 0x + Mx ---------- */
  function addressFromPublicKey(pub) {
    var script = "RETURN SIGNEDBY(0x" + toHexUpper(pub) + ")";
    // single-leaf MMR of the script MiniString
    var addr = hashConcat([serMiniNumber(0), serMiniData(utf8Bytes(script)), serMiniNumber(0)]);
    return { hex: "0x" + toHexUpper(addr), mx: minimaAddress(addr), script: script, addrBytes: addr };
  }
  function minimaAddress(addr) {                    // encode32( 01 || short(len) || addr || SHA3(addr)[0..4] )
    var checksum = SHA3(addr).slice(0, 4);
    var data = [0x01, (addr.length >> 8) & 0xFF, addr.length & 0xFF].concat(addr, checksum);
    return encode32(data);
  }
  // BaseConverter.encode32: BigInteger(1,data).toString(32) (0-9a-v), remap i->w l->y o->z, "Mx"+UPPER.
  var B32 = "0123456789abcdefghijklmnopqrstuv";
  function encode32(data) {
    var n = data.slice();
    var s = 0; while (s < n.length && n[s] === 0) s++;   // BigInteger(1,·) ignores leading zeros
    n = n.slice(s);
    var digits = "";
    if (!n.length) {
      digits = "0";
    } else {
      while (n.length) {
        var rem = 0, q = [], k;
        for (k = 0; k < n.length; k++) {
          var cur = (rem << 8) | n[k];
          q.push((cur >> 5) & 0xFF);                 // / 32
          rem = cur & 31;                            // % 32
        }
        var qs = 0; while (qs < q.length && q[qs] === 0) qs++;
        n = q.slice(qs);
        digits = B32[rem] + digits;
      }
    }
    digits = digits.replace(/i/g, "w").replace(/l/g, "y").replace(/o/g, "z");
    return "Mx" + digits.toUpperCase();
  }

  /* ---------- one full address for base seed + index ---------- */
  // keysPerLevel: 64 (classic, default) or 128 (block-as-keyuses).
  function deriveAddress(baseSeed, index, onLeaf, keysPerLevel) {
    var kpl = keysPerLevel || 64;
    var priv = privateSeed(baseSeed, index);
    var pub = treeKeyPublicKey(priv, onLeaf, kpl);
    var a = addressFromPublicKey(pub);
    a.index = index;
    a.keysPerLevel = kpl;
    a.publickey = "0x" + toHexUpper(pub);
    return a;   // { index, keysPerLevel, publickey, hex, mx, script }
  }

  root.KeyUsesDerive = {
    setSha3: setSha3,
    seedFromPhrase: seedFromPhrase,
    cleanSeedPhrase: cleanSeedPhrase,
    deriveAddress: deriveAddress,
    // exposed for the golden-vector tests:
    _privateSeed: privateSeed,
    _treeKeyPublicKey: treeKeyPublicKey,
    _wotsPublicKey: wotsPublicKey,
    _addressFromPublicKey: addressFromPublicKey,
    _modifierBytes: modifierBytes,
    _toHexUpper: toHexUpper,
    _fromHex: fromHex,
    _encode32: encode32
  };
  if (typeof module !== "undefined" && module.exports) module.exports = root.KeyUsesDerive;

})(typeof self !== "undefined" ? self : (typeof globalThis !== "undefined" ? globalThis : this));
