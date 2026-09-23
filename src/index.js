const ADJECTIVES = [
  "静かな", "素早い", "明るい", "涼しい", "温かい", "小さな", "大きな", "遠い",
  "近い", "澄んだ", "柔らかい", "鋭い", "陽気な", "穏やかな", "軽やかな", "力強い"
];

const ANIMALS = [
  "キツネ", "タヌキ", "ウサギ", "フクロウ", "リス", "カワウソ", "ハリネズミ",
  "ペンギン", "イルカ", "コアラ", "パンダ", "ツバメ", "カモシカ", "ヤマネコ"
];

function randomDeviceName(seed) {
  const a = ADJECTIVES[seed % ADJECTIVES.length];
  const b = ANIMALS[Math.floor(seed / ADJECTIVES.length) % ANIMALS.length];
  const n = (seed % 90) + 10;
  return `${a}${b}${n}`;
}

const PING_INTERVAL_MS = 5000;
const SESSION_TIMEOUT_MS = 12000;

export class NetworkRoom {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.sessions = new Map();
    this.sweepTimer = null;
  }

  startSweeper() {
    if (this.sweepTimer) return;
    this.sweepTimer = setInterval(() => this.sweep(), PING_INTERVAL_MS);
  }

  stopSweeperIfIdle() {
    if (this.sessions.size === 0 && this.sweepTimer) {
      clearInterval(this.sweepTimer);
      this.sweepTimer = null;
    }
  }

  sweep() {
    const now = Date.now();
    for (const session of Array.from(this.sessions.values())) {
      if (now - session.lastSeen > SESSION_TIMEOUT_MS) {
        this.dropSession(session.id, 1001, "timeout");
        continue;
      }
      try {
        session.socket.send(JSON.stringify({ type: "ping", t: now }));
      } catch (err) {
        this.dropSession(session.id, 1011, "send-failed");
      }
    }
    this.stopSweeperIfIdle();
  }

  dropSession(id, code, reason) {
    const session = this.sessions.get(id);
    if (!session) return;
    this.sessions.delete(id);
    try {
      session.socket.close(code, reason);
    } catch (err) {}
    this.broadcast({ type: "peer-left", id }, id);
    this.stopSweeperIfIdle();
  }

  async fetch(request) {
    const upgradeHeader = request.headers.get("Upgrade");
    if (!upgradeHeader || upgradeHeader.toLowerCase() !== "websocket") {
      return new Response("WebSocket連携が必要です", { status: 426 });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    server.accept();

    const id = crypto.randomUUID();
    const seed = Array.from(id).reduce((acc, ch) => acc + ch.charCodeAt(0), 0);
    const name = randomDeviceName(seed);

    const session = { id, name, socket: server, lastSeen: Date.now() };
    this.sessions.set(id, session);
    this.startSweeper();

    server.addEventListener("message", (event) => {
      session.lastSeen = Date.now();
      this.handleMessage(session, event.data);
    });

    const closeOrError = () => {
      this.dropSession(id, 1000, "closed");
    };
    server.addEventListener("close", closeOrError);
    server.addEventListener("error", closeOrError);

    server.send(JSON.stringify({ type: "welcome", id, name }));

    const peers = Array.from(this.sessions.values())
      .filter((s) => s.id !== id)
      .map((s) => ({ id: s.id, name: s.name }));
    server.send(JSON.stringify({ type: "peer-list", peers }));

    this.broadcast({ type: "peer-joined", id, name }, id);

    return new Response(null, { status: 101, webSocket: client });
  }

  handleMessage(session, raw) {
    let data;
    try {
      data = JSON.parse(raw);
    } catch (err) {
      return;
    }

    if (data.type === "ping") {
      try {
        session.socket.send(JSON.stringify({ type: "pong" }));
      } catch (err) {
        this.dropSession(session.id, 1011, "send-failed");
      }
      return;
    }

    if (data.type === "pong") {
      return;
    }

    if (data.type === "signal" && typeof data.to === "string") {
      const target = this.sessions.get(data.to);
      if (target) {
        try {
          target.socket.send(
            JSON.stringify({
              type: "signal",
              from: session.id,
              fromName: session.name,
              payload: data.payload,
            })
          );
        } catch (err) {
          this.dropSession(target.id, 1011, "send-failed");
        }
      }
    }
  }

  broadcast(message, excludeId) {
    const text = JSON.stringify(message);
    for (const session of this.sessions.values()) {
      if (session.id === excludeId) continue;
      try {
        session.socket.send(text);
      } catch (err) {
        this.dropSession(session.id, 1011, "send-failed");
      }
    }
  }
}

function expandIpv6(ip) {
  let addr = ip.split("%")[0];
  const v4Match = addr.match(/(\d+\.\d+\.\d+\.\d+)$/);
  if (v4Match) {
    const octets = v4Match[1].split(".").map(Number);
    const hex1 = ((octets[0] << 8) | octets[1]).toString(16);
    const hex2 = ((octets[2] << 8) | octets[3]).toString(16);
    addr = addr.replace(v4Match[1], `${hex1}:${hex2}`);
  }

  const halves = addr.split("::");
  const head = halves[0] ? halves[0].split(":") : [];
  const tail = halves.length > 1 && halves[1] ? halves[1].split(":") : [];
  const missing = 8 - head.length - tail.length;
  const groups = halves.length > 1
    ? [...head, ...Array(Math.max(missing, 0)).fill("0"), ...tail]
    : head;

  return groups.map((g) => g.padStart(4, "0"));
}

function networkIdFromRequest(request) {
  const ip = (request.headers.get("CF-Connecting-IP") || "unknown").trim();

  if (ip.includes(":")) {
    const groups = expandIpv6(ip);
    if (groups.length === 8) {
      return "v6_" + groups.slice(0, 4).join("");
    }
  }

  return "v4_" + ip.replace(/[^a-zA-Z0-9.]/g, "_");
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === "/ws") {
      const networkId = networkIdFromRequest(request);
      const objId = env.NETWORK_ROOM.idFromName(networkId);
      const stub = env.NETWORK_ROOM.get(objId);
      return stub.fetch(request);
    }

    return new Response("Not found", { status: 404 });
  },
};
